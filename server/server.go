package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
)

const (
	pingTimeout             = time.Second
	cameraInactivityTimeout = 5 * time.Second
)

type ping_status struct {
	ping_sent int
	deadline  time.Time
}

type ConnectionState struct {
	ping                ping_status
	conn_reader         bufio.Reader
	conn                net.Conn
	node_addr           string
	camera_addr         string
	healthy             bool
	remote_camera_count int
}

// cluster node
type ClusterNode struct {
	// mu protects the connection list and all mutable ConnectionState fields.
	mu sync.RWMutex

	connections []ConnectionState

	// Addresses advertised to other nodes and cameras.
	control_addr string
	camera_addr  string

	// listener for cluster control connections
	listening_conn *net.TCPListener
}

// represents all the states needed by ClusterCron
type ClusterCronStates struct {
	camera_conn       *net.UDPConn
	kafka_conns       []*kafka.Conn
	current_partition int

	cameraMu       sync.Mutex
	cameraLastSeen map[string]time.Time
}

func (s *ClusterCronStates) noteCamera(address string) {
	if address == "" {
		return
	}

	s.cameraMu.Lock()
	s.cameraLastSeen[address] = time.Now()
	s.cameraMu.Unlock()
}

// activeCameraCount returns the number of camera endpoints seen within the
// inactivity window. It is an estimate, not a reservation or hard capacity.
func (s *ClusterCronStates) activeCameraCount() int {
	now := time.Now()

	s.cameraMu.Lock()
	defer s.cameraMu.Unlock()

	for address, lastSeen := range s.cameraLastSeen {
		if now.Sub(lastSeen) > cameraInactivityTimeout {
			delete(s.cameraLastSeen, address)
		}
	}

	return len(s.cameraLastSeen)
}

func validHostPort(address string) bool {
	_, _, err := net.SplitHostPort(address)
	return err == nil
}

/*
Cluster configuration uses one peer per line:

control-address|camera-udp-address

Each node initiates a TCP connection only to peers whose control address sorts
after its own. The PEER handshake lets the receiving node identify the peer.
*/
func parse_cli_req(req []byte, node *ClusterNode) bool {
	lines := strings.Split(string(req), "\n")

	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" || line == "END" {
			break
		}

		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			log.Printf("Invalid peer entry %q; expected control-address|camera-address", line)
			return false
		}

		controlAddr := strings.TrimSpace(parts[0])
		cameraAddr := strings.TrimSpace(parts[1])
		if !validHostPort(controlAddr) || !validHostPort(cameraAddr) {
			log.Printf("Invalid peer address in entry %q", line)
			return false
		}
		if controlAddr == node.control_addr {
			log.Printf("Ignoring self entry %q", line)
			continue
		}

		// One full-duplex connection per pair: the lexicographically lower
		// control address initiates the connection.
		if node.control_addr >= controlAddr {
			continue
		}

		conn, err := net.DialTimeout("tcp4", controlAddr, time.Second)
		if err != nil {
			log.Printf("Error adding outbound connection to %s: %v", controlAddr, err)
			continue
		}

		if _, err := fmt.Fprintf(conn, "PEER %s|%s\n", node.control_addr, node.camera_addr); err != nil {
			log.Printf("Error introducing this node to %s: %v", controlAddr, err)
			conn.Close()
			continue
		}

		state := ConnectionState{
			ping:                ping_status{},
			conn_reader:         *bufio.NewReader(conn),
			conn:                conn,
			node_addr:           controlAddr,
			camera_addr:         cameraAddr,
			remote_camera_count: 0,
		}
		node.mu.Lock()
		node.connections = append(node.connections, state)
		node.mu.Unlock()
	}

	return true
}

func parsePeerHello(line string) (controlAddr, cameraAddr string, ok bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "PEER ") {
		return "", "", false
	}

	parts := strings.SplitN(strings.TrimPrefix(line, "PEER "), "|", 2)
	if len(parts) != 2 {
		return "", "", false
	}

	controlAddr = strings.TrimSpace(parts[0])
	cameraAddr = strings.TrimSpace(parts[1])
	if !validHostPort(controlAddr) || !validHostPort(cameraAddr) {
		return "", "", false
	}

	return controlAddr, cameraAddr, true
}

func appendAcceptedPeer(node *ClusterNode, conn net.Conn, reader *bufio.Reader, hello string) bool {
	controlAddr, cameraAddr, ok := parsePeerHello(hello)
	if !ok {
		return false
	}

	node.mu.Lock()
	node.connections = append(node.connections, ConnectionState{
		ping:        ping_status{},
		conn_reader: *reader,
		conn:        conn,
		node_addr:   controlAddr,
		camera_addr: cameraAddr,
	})
	node.mu.Unlock()
	return true
}

/*
Waits for cluster configuration. Incoming node connections may arrive before
the CLI configuration request, so they are identified by their PEER handshake.
*/
func wait_for_clireq(node *ClusterNode) {
	log.Println("\nWAITING FOR CLI TOOL TO SPECIFY CLUSTER")

	for {
		cliReq, err := node.listening_conn.Accept()
		if err != nil {
			log.Println("Error accepting cluster connection:", err)
			continue
		}

		_ = cliReq.SetDeadline(time.Now().Add(2 * time.Second))
		reader := bufio.NewReader(cliReq)
		firstLine, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Error reading initial cluster message:", err)
			cliReq.Close()
			continue
		}

		if strings.HasPrefix(strings.TrimSpace(firstLine), "PEER ") {
			if !appendAcceptedPeer(node, cliReq, reader, firstLine) {
				log.Printf("Invalid peer handshake from %s", cliReq.RemoteAddr())
				cliReq.Close()
				continue
			}
			_ = cliReq.SetDeadline(time.Time{})
			log.Printf("Accepted peer connection from %s", cliReq.RemoteAddr())
			continue
		}

		if strings.TrimSpace(firstLine) != "CONFIG" {
			log.Printf("Invalid initial cluster message from %s", cliReq.RemoteAddr())
			cliReq.Close()
			continue
		}

		var request strings.Builder
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil {
				log.Println("Error reading CLI cluster request:", readErr)
				break
			}
			if strings.TrimSpace(line) == "END" {
				if parse_cli_req([]byte(request.String()), node) {
					response := "OK FORMED A CLUSTER\n"
					if _, err := fmt.Fprint(cliReq, response); err != nil {
						log.Println("Error responding to CLI:", err)
					}
					log.Print(response)
					cliReq.Close()
					return
				}
				break
			}
			request.WriteString(line)
		}

		if err := cliReq.Close(); err != nil {
			log.Println("Error closing CLI connection:", err)
		}
	}
}

/* pushes an event into a Kafka partition */
func produce_event(conn *kafka.Conn, event types.Packet) {
	var send_event []byte
	idSlice := make([]byte, 8)
	binary.BigEndian.PutUint64(idSlice, event.CameraID)
	send_event = append(send_event, idSlice...)

	send_event = binary.BigEndian.AppendUint32(
		send_event,
		uint32(len(event.CameraAddress)),
	)
	send_event = append(send_event, event.CameraAddress...)

	send_event = binary.BigEndian.AppendUint32(
		send_event,
		uint32(len(event.Data)),
	)
	send_event = append(send_event, event.Data[:]...)

	_, err := conn.WriteMessages(kafka.Message{Value: send_event})
	if err != nil {
		log.Printf("Error queuing packet: %v", err)
	}
}

/*
Takes feed from cameras and pushes events into Kafka.
*/
func ingestion(states *ClusterCronStates) {
	cameraConn := states.camera_conn
	buf := make([]byte, 1024)

	// Do not let an idle camera socket stall cluster health checks forever.
	if err := cameraConn.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		log.Println("could not set camera read deadline:", err)
		return
	}
	n, _, err := cameraConn.ReadFromUDP(buf)
	if err != nil {
		var timeoutErr net.Error
		if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
			return
		}
		log.Println("error receiving packet:", err)
		return
	}

	raw := buf[:n]
	if len(raw) < 12 {
		log.Println("packet too small")
		return
	}

	var packet types.Packet
	packet.CameraID = binary.BigEndian.Uint64(raw[0:8])

	addressLen := int(binary.BigEndian.Uint32(raw[8:12]))
	addressStart := 12
	addressEnd := addressStart + addressLen
	if addressLen < 0 || addressEnd+4 > len(raw) {
		log.Println("invalid address length")
		return
	}

	packet.CameraAddress = string(raw[addressStart:addressEnd])

	dataLen := int(binary.BigEndian.Uint32(raw[addressEnd : addressEnd+4]))
	dataStart := addressEnd + 4
	dataEnd := dataStart + dataLen
	if dataEnd > len(raw) || dataLen > len(packet.Data) {
		log.Println("invalid data length")
		return
	}

	copy(packet.Data[:], raw[dataStart:dataEnd])
	states.noteCamera(packet.CameraAddress)

	partition := states.current_partition
	fmt.Printf("Partition: %d\n", partition)
	produce_event(states.kafka_conns[partition], packet)

	states.current_partition++
	if states.current_partition >= types.MAX_PARTITION {
		states.current_partition = 0
	}
}

/* Reads with a deadline so health checks don't block indefinitely. */
func ReadWithDeadline(state *ConnectionState) string {
	if err := state.conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		log.Println("Couldn't set read deadline:", err)
		return ""
	}

	defer func() {
		if err := state.conn.SetReadDeadline(time.Time{}); err != nil {
			log.Println("Couldn't clear read deadline:", err)
		}
	}()

	message, err := state.conn_reader.ReadString('\n')
	if err != nil {
		var timeoutErr net.Error
		if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
			return ""
		}
		if !errors.Is(err, net.ErrClosed) {
			log.Println("Error reading from connection:", err)
		}
		return ""
	}

	return strings.TrimSpace(message)
}

func reportedCameraCount(message, command string) (int, bool) {
	fields := strings.Fields(message)
	if len(fields) != 2 || fields[0] != command {
		return 0, false
	}

	count, err := strconv.Atoi(fields[1])
	if err != nil || count < 0 {
		return 0, false
	}
	return count, true
}

/*
PING and PONG carry each node's recently active camera count. The count is
best-effort cluster state; it is not a capacity reservation.
*/
func check_health(node *ClusterNode, localCameraCount int) {
	node.mu.Lock()
	defer node.mu.Unlock()

	for i := range node.connections {
		state := &node.connections[i]
		if state.conn == nil {
			continue
		}

		switch state.ping.ping_sent {
		case 0:
			if _, err := fmt.Fprintf(state.conn, "PING %d\n", localCameraCount); err != nil {
				log.Printf("Error sending PING to node %s: %v", state.node_addr, err)
				state.healthy = false
				state.ping = ping_status{}
				continue
			}
			state.ping.ping_sent = 1
			state.ping.deadline = time.Now().Add(pingTimeout)

		case 1:
			message := ReadWithDeadline(state)
			if message != "" {
				fields := strings.Fields(message)
				switch {
				case len(fields) >= 1 && fields[0] == "PING":
					if count, ok := reportedCameraCount(message, "PING"); ok {
						state.remote_camera_count = count
					}
					state.healthy = true
					if _, err := fmt.Fprintf(state.conn, "PONG %d\n", localCameraCount); err != nil {
						log.Printf("Error responding to PING from %s: %v", state.node_addr, err)
						state.healthy = false
					}

				case len(fields) >= 1 && fields[0] == "PONG":
					if count, ok := reportedCameraCount(message, "PONG"); ok {
						state.remote_camera_count = count
					}
					state.healthy = true
					state.ping = ping_status{}
				}
			}

			if state.ping.ping_sent == 1 && time.Now().After(state.ping.deadline) {
				log.Printf("Peer health check timed out: %s", state.node_addr)
				state.healthy = false
				state.ping = ping_status{}
			}
		}
	}
}

/* Main loop for this server. */
func clusterCron(node *ClusterNode, states *ClusterCronStates) {
	for {
		// Accept node-to-node connections. Each accepted connection must begin
		// with a PEER handshake before being treated as a cluster connection.
		err := node.listening_conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if err != nil {
			log.Println("Couldn't set deadline on listening socket:", err)
		} else {
			newInbound, acceptErr := node.listening_conn.Accept()
			if acceptErr != nil {
				var timeoutErr net.Error
				if errors.As(acceptErr, &timeoutErr) && timeoutErr.Timeout() {
					// No new connection during this iteration.
				} else {
					log.Println("Error accepting new inbound client:", acceptErr)
				}
			} else {
				_ = newInbound.SetDeadline(time.Now().Add(time.Second))
				reader := bufio.NewReader(newInbound)
				hello, readErr := reader.ReadString('\n')
				if readErr != nil || !appendAcceptedPeer(node, newInbound, reader, hello) {
					log.Printf("Rejecting connection without a valid PEER handshake from %s", newInbound.RemoteAddr())
					newInbound.Close()
				} else {
					_ = newInbound.SetDeadline(time.Time{})
				}
			}
		}

		ingestion(states)
		check_health(node, states.activeCameraCount())
		time.Sleep(100 * time.Millisecond)
	}
}

func deriveCameraAddress(advertisedAddress, controlAddress string, cameraPort int) (string, error) {
	if advertisedAddress != "" {
		if !validHostPort(advertisedAddress) {
			return "", fmt.Errorf("invalid -camera_address %q; expected host:port", advertisedAddress)
		}
		return advertisedAddress, nil
	}

	host, _, err := net.SplitHostPort(controlAddress)
	if err != nil {
		return "", fmt.Errorf("cannot derive camera address from -server_address: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return "", fmt.Errorf("set -camera_address when -server_address binds to a wildcard address")
	}
	return net.JoinHostPort(host, strconv.Itoa(cameraPort)), nil
}

func main() {
	listeningAddr := flag.String("server_address", "nil", "TCP address used for cluster control")
	cameraPort := flag.Int("cam_addr", 0, "UDP port on which this server receives camera traffic")
	cameraAddressFlag := flag.String("camera_address", "", "advertised UDP address for cameras (defaults to host in -server_address plus -cam_addr)")
	assignmentListenAddr := flag.String("assign_listen_addr", "", "TCP address for camera assignment requests")
	flag.Parse()

	if *cameraPort < 1 || *cameraPort > 65535 {
		log.Fatal("-cam_addr must be a TCP/UDP port in the range 1..65535")
	}

	cameraAddress, err := deriveCameraAddress(*cameraAddressFlag, *listeningAddr, *cameraPort)
	if err != nil {
		log.Fatal(err)
	}

	addr, err := net.ResolveTCPAddr("tcp4", *listeningAddr)
	if err != nil {
		log.Fatal("Unable to resolve server address:", err)
	}

	listenConn, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		log.Fatal("Unable to listen for cluster connections:", err)
	}

	node := ClusterNode{
		listening_conn: listenConn,
		control_addr:   *listeningAddr,
		camera_addr:    cameraAddress,
	}
	clusterState := ClusterCronStates{
		cameraLastSeen: make(map[string]time.Time),
	}

	wait_for_clireq(&node)

	// Listen for camera data on UDP.
	cameraConn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: *cameraPort})
	if err != nil {
		log.Fatal(err)
	}
	defer cameraConn.Close()

	// Connect to Kafka partitions.
	const topic = "camera-topic"
	kafkaConns := make([]*kafka.Conn, types.MAX_PARTITION)
	for partition := 0; partition < types.MAX_PARTITION; partition++ {
		kafkaConn, dialErr := kafka.DialLeader(
			context.Background(),
			"tcp",
			"localhost:9092",
			topic,
			partition,
		)
		if dialErr != nil {
			log.Fatalf("failed to connect to partition %d: %v", partition, dialErr)
		}
		kafkaConns[partition] = kafkaConn
	}
	defer func() {
		for _, kafkaConn := range kafkaConns {
			_ = kafkaConn.Close()
		}
	}()

	clusterState.camera_conn = cameraConn
	clusterState.kafka_conns = kafkaConns
	clusterState.current_partition = 0

	if *assignmentListenAddr != "" {
		assignmentTCPAddr, resolveErr := net.ResolveTCPAddr("tcp4", *assignmentListenAddr)
		if resolveErr != nil {
			log.Fatal("Unable to resolve assignment listener address:", resolveErr)
		}
		assignmentListener, listenErr := net.ListenTCP("tcp4", assignmentTCPAddr)
		if listenErr != nil {
			log.Fatal("Unable to listen for camera assignments:", listenErr)
		}
		defer assignmentListener.Close()
		go serveAssignment(assignmentListener, &node, cameraAddress, &clusterState)
		log.Printf("Camera assignment service listening on %s; advertises %s", *assignmentListenAddr, cameraAddress)
	}

	clusterCron(&node, &clusterState)
}
