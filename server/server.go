package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strings"
	"time"
)

// cluster node
type ClusterNode struct {
	out_bound_connections []net.Conn
	in_bound_connections  []net.Conn
	listening_conn        net.Listener
}

// represents all the states needed by ClusterCron
type ClusterCronStates struct {
	camera_conn       net.UDPConn
	kafka_conns       []kafka.Conn
	current_partition int
}

/*
	Cluster initialization functions
*/

/*
Fills Node List, if successful returns true else false

Request format is:

some_cli_unique_identifier (not decided yet)\r\n
1st node \r\n
2nd node \r\n
this : this node \r\n
...
\r\n\r\n
*/
func parse_cli_req(req []byte, node *ClusterNode, this_node_addr *string) bool {
	lines := strings.Split(string(req), "\r\n")

	identifier := lines[0]

	// TODO(Me): check if correct identifier
	if identifier == "" {
		return false
	}

	for _, line := range lines[1:] {
		if line == "" {
			break
		}

		// TODO(AI Suggest): this_node_addr = assign this node addr

		// TODO(Me): return false when line is wrong

		conn, err := net.Dial("tcp4", line)

		if err != nil {
			log.Println("Error Adding Outbound Connectino")
		}

		node.out_bound_connections = append(node.out_bound_connections, &conn)
	}
	return true
}

/* Waits for correct cli request, adds outbound connections and send appropiate message back to cli tool */
func wait_for_clireq(node *ClusterNode) string {
	log.Println("\nWAITING FOR CLI TOOL TO SPECIFY CLUSTER")
	waiting_for_cli, err := net.Listen("tcp4", "localhost:8000")

	if err != nil {
		fmt.Println("Unable to listen for cli tool")
	}

	for {
		cli_req, err := waiting_for_cli.Accept()

		cli_msg := make([]byte, 1024)
		n, err := cli_req.Read(cli_msg) // TODO(AI): loop for complete message

		cli_req.Close()
		var response_to_cli string
		var this_node_addr string

		if parse_cli_req(cli_msg[:n], node, &this_node_addr) {
			response_to_cli = "\nOK FORMED A CLUSTER"

			_, err := cli_req.Write([]byte(response_to_cli)) // TODO(AI): loop for complete message
			if err != nil {
				log.Println("Error sending response to cli")
			}

			log.Println(response_to_cli)
			waiting_for_cli.Close()
			return this_node_addr
		}

		response_to_cli = "\nINVAILD CLI REQUEST"
		_, err = cli_req.Write([]byte(response_to_cli)) // TODO(AI): loop for complete message
		if err != nil {
			log.Println("\nError sending response to cli")
		}
		log.Println(response_to_cli)
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

/* Takes feed from cctv and push events into kafka
 */
func ingestion(camera_conn *net.UDPConn, kafka_conns []*kafka.Conn, current_partition int) {
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

	fmt.Printf("Partition: %d\n", current_partition)

	produce_event(
		kafka_conns[current_partition],
		p,
	)

	current_partition++

	if current_partition >= types.MAX_PARTITION {
		current_partition = 0
	}
}

func (n *ClusterNode) pingAll() {
	for nodes := range n.others_links {

	}
}

/* Gets called 10 times in 1s. This is the main loop for this server */
func clusterCron(node *ClusterNode, states *ClusterCronStates) {
	for {
		// accept incoming connections
		node.listening_conn.Accept()

		ingestion(states.camera_conn, states.kafka_conns, states.current_partition)

		ping_all()
		time.Sleep(time.Millisecond * 100)
	}
}

func main() {

	var node ClusterNode

	// Form outbound connection with other servers
	listening_addr := wait_for_clireq(&node)

	// add inbound connection
	other_server_conn, err := net.Listen("tcp4", listening_addr)
	node.listening_conn = &other_server_conn

	if err != nil {
		log.Fatal(err)
	}

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

	current_partition := 0

	clusterCron()
}
