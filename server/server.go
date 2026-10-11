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
	"sync"
	"time"

	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
)

// cluster node
type ClusterNode struct {
	// connections
	connections []*net.Conn

	// One persistent reader per connection, so buffered bytes are not lost
	// between cron iterations.
	readerMu sync.Mutex
	readers  map[net.Conn]*bufio.Reader

	// listener for cluster
	listening_conn *net.TCPListener
}
// represents all the states needed by ClusterCron
type ClusterCronStates struct {
	camera_conn       *net.UDPConn
	kafka_conns       []*kafka.Conn
	current_partition int
}

/*
	Cluster initialization functions
*/

/*
Fills out_bound_connections

Request format is:

1st node \r\n
2nd node \r\n
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
		node.connections = append(node.connections, &conn)
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

/*
	Cron functions
*/

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
func ingestion(camera_conn *net.UDPConn, kafka_conns []*kafka.Conn, current_partition *int) {
	buf := make([]byte, 1024)

	n, _, err := camera_conn.ReadFromUDP(buf)
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

/*
on-wire format of PING : "PING\n" & same for PONG

returns addr of node which did not respond pong

NOTE : PING and PONG are both followed by \n as there might be other messsages, hence having \n delimiter
*/
func (n *ClusterNode) pingAll() []string {
	var ws sync.WaitGroup
	var fault_pongs []string
	var fp_mut sync.Mutex

	for i := 0; i < len(n.connections); i++ {
		conn := *n.connections[i]
		ws.Go(func() {
			if _, err := fmt.Fprintf(conn, "PING\n"); err != nil {
				log.Println("Error sending PING:", err)
				fp_mut.Lock()
				fault_pongs = append(fault_pongs, conn.RemoteAddr().String())
				fp_mut.Unlock()
				return
			}

			// Reuse the reader for this connection across cron iterations.
			n.readerMu.Lock()
			if n.readers == nil {
				n.readers = make(map[net.Conn]*bufio.Reader)
			}
			reader := n.readers[conn]
			if reader == nil {
				reader = bufio.NewReader(conn)
				n.readers[conn] = reader
			}
			n.readerMu.Unlock()

			// Bound every read, including the first one.
			if err := conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond)); err != nil {
				log.Println("Couldn't set read deadline:", err)
				return
			}
			defer func() {
				if err := conn.SetReadDeadline(time.Time{}); err != nil {
					log.Println("Couldn't clear read deadline:", err)
				}
			}()

			// Read one message first. If it is a peer PING, reply before
			// waiting for the PONG corresponding to our own PING.
			message, err := reader.ReadString('\n')
			if err != nil {
				fp_mut.Lock()
				fault_pongs = append(fault_pongs, conn.RemoteAddr().String())
				fp_mut.Unlock()

				var timeoutErr net.Error
				if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
					log.Printf("Read timed out waiting for a message from %s", conn.RemoteAddr())
				} else {
					log.Printf("Error receiving message from %s: %v", conn.RemoteAddr(), err)
				}
				return
			}

			switch strings.TrimSuffix(message, "\n") {
			case "PING":
				if _, err := fmt.Fprintf(conn, "PONG\n"); err != nil {
					log.Println("Error sending PONG:", err)
					return
				}

				// After replying to the peer's PING, wait briefly for
				// the PONG to our own PING.
				message, err = reader.ReadString('\n')
				if err != nil {
					fp_mut.Lock()
					fault_pongs = append(fault_pongs, conn.RemoteAddr().String())
					fp_mut.Unlock()

					var timeoutErr net.Error
					if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
						log.Printf("Read timed out waiting for PONG from %s", conn.RemoteAddr())
					} else {
						log.Printf("Error receiving PONG from %s: %v", conn.RemoteAddr(), err)
					}
					return
				}
				if strings.TrimSuffix(message, "\n") != "PONG" {
					log.Printf("Expected PONG from %s, got %q", conn.RemoteAddr(), strings.TrimSuffix(message, "\n"))
					fp_mut.Lock()
					fault_pongs = append(fault_pongs, conn.RemoteAddr().String())
					fp_mut.Unlock()
				}
			case "PONG":
				// The peer has already answered our PING.
				return
			default:
				log.Printf("Unexpected message from %s: %q", conn.RemoteAddr(), strings.TrimSuffix(message, "\n"))
				fp_mut.Lock()
				fault_pongs = append(fault_pongs, conn.RemoteAddr().String())
				fp_mut.Unlock()
			}
		})
	}
	ws.Wait()
	return fault_pongs
}
/* Sleeps for 100ms after every iteration. This is the main loop for this server (add go routine to ping to make it concurrent) */
func clusterCron(node *ClusterNode, states *ClusterCronStates) {
	for {
		//log.Printf("Number of connections in out_bound : %d and in_bound : %d", len(node.connections), len(node.in_bound_connections)) // both should have 1 for 2 servers

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
				node.connections = append(node.connections, &new_inbound_client)
			}
		}

		// send new events (TODO (me): how much time does this func takes in ms)
		ingestion(states.camera_conn, states.kafka_conns, &states.current_partition)

		// PING
		for _, fn := range node.pingAll() {
			// TODO: set in pfail or fail
			log.Printf("Did not recieve a PONG from %s", fn)
		}

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

	defer cam_conn.Close() // in cleanup func

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
			log.Fatalf(
				"failed to connect to partition %d: %v",
				partition,
				err,
			)
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
