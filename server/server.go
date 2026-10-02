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
	// connection to other servers
	out_bound_connections []*net.Conn

	// connection made by other servers
	in_bound_connections []*net.Conn

	// listening_conn for cluster
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

	//identifier := lines[0]
	// TODO(Me): check if correct identifier
	//if identifier == "" {
	//	return false
	//}

	for _, line := range lines[1:] {
		if line == "" {
			break
		}

		// TODO(AI Suggest): this_node_addr = assign this node addr

		// TODO(Me): return false when line is wrong

		conn, err := net.Dial("tcp4", line)

		if err != nil {
			log.Println("Error Adding Outbound Connection:", err)
			continue
		}

		node.out_bound_connections = append(node.out_bound_connections, &conn)
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
			if _, err := cli_req.Write([]byte(response_to_cli)); err != nil {
				log.Println("Error sending response to cli:", err)
			}
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
on-wire format of PING : "addr_of_this_server\nPING\n" & same for PONG
returns addr of node which did not respond pong
*/
func (n *ClusterNode) pingAll() []string {
	var ws sync.WaitGroup
	var fault_pongs []string
	var fp_mut sync.Mutex

	// make routine, wait for first response (have a deadline), now check correctness, only now get the lock once and add if fault.

	for i := 0; i < len(n.out_bound_connections); i++ {
		conn := *n.out_bound_connections[i]
		ws.Go(func() {
			if _, err := fmt.Fprintf(conn, "%s\nPING\n", conn.LocalAddr().String()); err != nil {
				log.Println("Error sending PING:", err)
				return
			}

			if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
				log.Println("Couldn't set read deadline:", err)
				return
			}

			scanner := bufio.NewScanner(conn)

			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						log.Println("PONG timeout")
					} else {
						log.Println("Error receiving PONG:", err)
					}
					fp_mut.Lock()
					fault_pongs = append(fault_pongs, conn.RemoteAddr().String())
					fp_mut.Unlock()
				}
				return
			}
			pongAddr := scanner.Text()

			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					log.Println("Error receiving PONG:", err)
					fp_mut.Lock()
					fault_pongs = append(fault_pongs, conn.RemoteAddr().String())
					fp_mut.Unlock()
				}
				return
			}
			pong := scanner.Text()

			if pong != "PONG" {
				fmt.Printf("Incorrect PONG received from %s\n", pongAddr)
				fp_mut.Lock()
				fault_pongs = append(fault_pongs, pongAddr)
				fp_mut.Unlock()
			}

			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				log.Println("Couldn't clear read deadline:", err)
			}
		})
	}
	ws.Wait()
	return fault_pongs
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
				node.in_bound_connections = append(node.in_bound_connections, &new_inbound_client)
			}
		}

		// send new events (TODO (me): how much time does this func takes in ms)
		ingestion(states.camera_conn, states.kafka_conns, &states.current_partition)

		// check on other servers
		for _, fn := range node.pingAll() {
			// TODO: set in pfail or fail
			log.Printf("This node did not send a PONG %s", fn)
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func main() {

	listening_addr := flag.String("server_address", "nil", "Specify address of server to start on")

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
		Port: 8080,
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
