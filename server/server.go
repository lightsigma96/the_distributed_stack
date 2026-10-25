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
	"strings"
	"time"

	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
)

const pingTimeout = time.Second

type ping_status struct {
	ping_sent int
	deadline  time.Time
}

type ConnectionState struct {
	ping        ping_status
	conn_reader bufio.Reader
	conn        net.Conn
	node_addr   string
}

// cluster node
type ClusterNode struct {
	// connections
	connections []ConnectionState

	// listener for cluster
	listening_conn *net.TCPListener
}

// represents all the states needed by ClusterCron
type ClusterCronStates struct {
	camera_conn       *net.UDPConn
	kafka_conns       []*kafka.Conn
	current_partition int

	camera_count     int
	camera_addresses map[string]struct{}
}

// record_camera records a camera endpoint once, even though that camera sends
// many packets. The UDP source address is used as the camera's observed identity.
func (s *ClusterCronStates) record_camera(address string) {
	if address == "" {
		return
	}

	if s.camera_addresses == nil {
		s.camera_addresses = make(map[string]struct{})
	}

	if _, exists := s.camera_addresses[address]; exists {
		return
	}

	s.camera_addresses[address] = struct{}{}
	s.camera_count++
}

// ReportCameraCount returns the number of distinct camera endpoints this
// server has observed since startup.
func (s *ClusterCronStates) ReportCameraCount() int {
	return s.camera_count
}

/*
	Cluster initialization functions
*/

/*
Fills out_bound_connections

Request format is:

1st node addr\r\n
2nd node addr\r\n
...
\r\n\r\n
*/
func parse_cli_req(req []byte, node *ClusterNode) bool {
	lines := strings.Split(string(req), "\r\n")

	for _, line := range lines[:] {
		if line == "" {
			break
		}

		// TODO(Me): return false when line is wrong

		conn, err := net.Dial("tcp4", line)

		if err != nil {
			log.Println("Error Adding Outbound Connection:", err)
			continue
		}

		// in bound should have listen and outbound dial

		node.connections = append(node.connections, ConnectionState{ping_status{}, *bufio.NewReader(conn), conn, line})
	}
	return true
}

/* Waits for correct cli request, sends appropiate message back to cli tool */
func wait_for_clireq(node *ClusterNode) {
	log.Println("\nWAITING FOR CLI TOOL TO SPECIFY CLUSTER")

	for {
		cli_req, err := node.listening_conn.Accept()
		if err != nil {
			log.Println("Error accepting CLI connection:", err)
			continue
		}

		cli_msg := make([]byte, 1024)
		n, err := cli_req.Read(cli_msg) // TODO(AI): loop for complete message
		if err != nil {
			log.Println("Error reading CLI request:", err)
			cli_req.Close()
			continue
		}

		if parse_cli_req(cli_msg[:n], node) {
			response_to_cli := "\nOK FORMED A CLUSTER"
			//if _, err := cli_req.Write([]byte(response_to_cli)); err != nil {
			//	log.Println("Error sending response to cli:", err)
			//}
			log.Println(response_to_cli)
			cli_req.Close()
			return
		}

		response_to_cli := "\nINVAILD CLI REQUEST"
		if _, err := cli_req.Write([]byte(response_to_cli)); err != nil {
			log.Println("Error sending response to cli:", err)
		}
		log.Println(response_to_cli)
		cli_req.Close()
	}
}

/* pushses a event into kafka partition */

func produce_event(conn *kafka.Conn, event types.Packet) {
	var send_event []byte
	id_slice := make([]byte, 8)
	binary.BigEndian.PutUint64(id_slice, event.CameraID)
	send_event = append(send_event, id_slice...)

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

	_, err := conn.WriteMessages(
		kafka.Message{
			Value: send_event,
		},
	)

	if err != nil {
		log.Printf("Error queuing packet: %v", err)
	}
}

/*
Takes feed from cctv and push events into kafka
*/
func ingestion(camera_conn *net.UDPConn, kafka_conns []*kafka.Conn, current_partition *int, states *ClusterCronStates) {
	buf := make([]byte, 1024)

	n, camera_addr, err := camera_conn.ReadFromUDP(buf)
	if err != nil {
		log.Println("error receiving packet:", err)
		return
	}

	raw := buf[:n]

	if len(raw) < 12 {
		log.Println("packet too small")
		return
	}

	var p types.Packet
	p.CameraID = binary.BigEndian.Uint64(raw[0:8])

	addressLen := int(binary.BigEndian.Uint32(raw[8:12]))
	addressStart := 12
	addressEnd := addressStart + addressLen

	if addressEnd+4 > len(raw) {
		log.Println("invalid address length")
		return
	}

	p.CameraAddress = string(raw[addressStart:addressEnd])

	dataLen := int(binary.BigEndian.Uint32(
		raw[addressEnd : addressEnd+4],
	))
	dataStart := addressEnd + 4
	dataEnd := dataStart + dataLen

	if dataEnd > len(raw) || dataLen > len(p.Data) {
		log.Println("invalid data length")
		return
	}

	copy(p.Data[:], raw[dataStart:dataEnd])
	states.record_camera(camera_addr.String())
	fmt.Printf("Partition: %d\n", *current_partition)

	produce_event(
		kafka_conns[*current_partition],
		p,
	)

	*current_partition++

	if *current_partition >= types.MAX_PARTITION {
		*current_partition = 0
	}
}

/* Reads with a particular deadline, purpose is that reads should not be blocking, handles errors internally */
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
			log.Println("Read timed out")
			return ""
		}
		if !errors.Is(err, net.ErrClosed) {
			log.Println("Error reading from connection:", err)
		}
		return ""
	}

	return strings.TrimSuffix(strings.TrimSuffix(message, "\n"), "\r")
}

/*
on-wire format of PING : "PING\n" & same for PONG

returns addr of node which did not respond pong

NOTE : PING and PONG are both followed by \n as there might be other messsages, hence having \n delimiter
*/
func check_health(n *ClusterNode) {
	/* Be self-centered, just do your task */

	for i := 0; i < len(n.connections); i++ {
		state := &n.connections[i]
		conn := state.conn

		switch state.ping.ping_sent {
		case 0:
			if _, err := fmt.Fprintf(conn, "PING\n"); err != nil {
				log.Printf("Error sending PING to node %s: %v", state.node_addr, err)
				continue
			}
			state.ping.ping_sent = 1
			state.ping.deadline = time.Now().Add(pingTimeout)

		case 1:
			node_msg := ReadWithDeadline(state)

			if !time.Now().After(state.ping.deadline) {
				if node_msg == "PONG" {
					log.Printf("RECEIVED PONG FROM: %s", state.node_addr)
					state.ping = ping_status{}
				} else {
					if node_msg == "PING" {
						if _, err := fmt.Fprintf(conn, "PONG\n"); err != nil {
							log.Printf("ERROR IN ACCOUNTING FOR PING FOR: %s", state.node_addr)
						}
					}
					// handle whatever the msg is
				}
			} else {
				log.Printf("FAILING NODE: %s", state.node_addr)
			}
		}
	}
}

/* Sleeps for 100ms after every iteration. This is the main loop for this server (add go routine to ping to make it concurrent) */
func clusterCron(node *ClusterNode, states *ClusterCronStates) {
	for {
		// accept incoming connections (10ms deadline)
		err := node.listening_conn.SetDeadline(time.Now().Add(time.Millisecond * 10))
		if err != nil {
			log.Println("Couldn't set deadline on listening socket:", err)
		} else {
			new_inbound_client, err := node.listening_conn.Accept()

			if err != nil {
				var timeoutErr net.Error
				if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
					log.Println("No new connection to accept")
				} else {
					log.Println("Error in accepting new inbound client:", err)
				}
			} else {
				node.connections = append(node.connections, ConnectionState{
					ping:        ping_status{},
					conn_reader: *bufio.NewReader(new_inbound_client),
					conn:        new_inbound_client,
				})
			}
		}

		// send new events (TODO (me): how much time does this func takes in ms)
		ingestion(states.camera_conn, states.kafka_conns, &states.current_partition, states)

		// Check Health
		check_health(node)

		time.Sleep(time.Millisecond * 100)
	}
}

func main() {
	listening_addr := flag.String("server_address", "nil", "Specify address of server to start on")
	cameras_addr := flag.Int("cam_addr", 0, "Specify address of camera to listen on")
	flag.Parse()

	addr, err := net.ResolveTCPAddr("tcp4", *listening_addr)
	if err != nil {
		log.Fatal("Unable to resolve server address:", err)
	}

	listen_conn, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		log.Fatal("Unable to listen for cli tool:", err)
	}

	var node ClusterNode
	var cluster_state ClusterCronStates
	node.listening_conn = listen_conn

	// become a part of cluster
	wait_for_clireq(&node)

	// Form connection with cameras
	const topic = "camera-topic"
	cam_conn, err := net.ListenUDP("udp4", &net.UDPAddr{
		Port: *cameras_addr,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cam_conn.Close()

	// Form connection with kafka
	kafka_conns := make([]*kafka.Conn, types.MAX_PARTITION)
	for partition := 0; partition < types.MAX_PARTITION; partition++ {
		kafka_conn, err := kafka.DialLeader(
			context.Background(),
			"tcp",
			"localhost:9092",
			topic,
			partition,
		)
		if err != nil {
			log.Fatalf("failed to connect to partition %d: %v", partition, err)
		}
		kafka_conns[partition] = kafka_conn
	}
	defer func() {
		for _, kafka_conn := range kafka_conns {
			kafka_conn.Close()
		}
	}()

	cluster_state.camera_conn = cam_conn
	cluster_state.kafka_conns = kafka_conns
	cluster_state.current_partition = 0
	clusterCron(&node, &cluster_state)
}
