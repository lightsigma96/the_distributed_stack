package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
)

// cluster node
type ClusterNode struct {
	out_bound_connections []*net.Conn

	in_bound_connections []*net.Conn

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
Fills out_bound_connections and set listening_conn for this server address

Request format is:

some_cli_unique_identifier (not decided yet)\r\n
1st node \r\n
2nd node \r\n
this : this node \r\n
...
\r\n\r\n
*/
func parse_cli_req(req []byte, node *ClusterNode) bool {
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

/* Waits for correct cli request, sends appropiate message back to cli tool */
func wait_for_clireq(node *ClusterNode) {
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

		if parse_cli_req(cli_msg[:n], node) {
			response_to_cli = "\nOK FORMED A CLUSTER"

			_, err := cli_req.Write([]byte(response_to_cli)) // TODO(AI): loop for complete message
			if err != nil {
				log.Println("Error sending response to cli")
			}

			log.Println(response_to_cli)
			waiting_for_cli.Close()
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

/*
Takes feed from cctv and push events into kafka
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

/*
on-wire format of PING : "addr_of_this_server\nPING\n" & same for PONG
returns addr of node which did not respond pong
*/
func (n *ClusterNode) pingAll() []string {
	var fault_pongs []string

	for i := 0; i < len(n.out_bound_connections); i++ {
		fmt.Fprintf(*n.out_bound_connections[i], "%s\n%s\n", (*n.out_bound_connections[i]).LocalAddr().String(), "PING")

		err := (*n.out_bound_connections[i]).SetReadDeadline(time.Now().Add(time.Millisecond * 20))
		if err != nil {
			log.Println("Couldn't set deadline on listening socket")
		}

		msg_reader := bufio.NewScanner((*n.out_bound_connections[i]))

		pong_addr := make([]byte, 0, 1024)
		for msg_reader.Scan() {
			// TODO (AI): handle msg too big
			pong_addr = msg_reader.Bytes()
		}

		pong := make([]byte, 0, 1024)
		for msg_reader.Scan() {
			// TODO (AI): handle msg too big
			pong = msg_reader.Bytes()
		}

		scn_err := msg_reader.Err()
		if !errors.Is(scn_err, io.EOF) {
			log.Println("Some error in recieving PONG")
		}

		if string(pong) != "PONG" {
			fmt.Printf("Incorrect PONG recieved from %s", string(pong_addr))
			fault_pongs = append(fault_pongs, string(pong_addr))
		}
	}
	return fault_pongs
}

/* Gets called 10 times in 1s that means each iteration runs for 100ms. This is the main loop for this server */
func clusterCron(node *ClusterNode, states *ClusterCronStates) {
	for {
		// accept incoming connections (10ms deadline)
		err := node.listening_conn.SetDeadline(time.Now().Add(time.Millisecond * 10))

		if err != nil {
			log.Println("Couldn't set deadline on listening socket")
		}

		new_inbound_client, err := node.listening_conn.Accept()

		if err != nil {
			var timeout_err net.Error
			if errors.As(err, timeout_err) && timeout_err.Timeout() {
				log.Println("No new connection to accept")
			}
			log.Println("Error in accepting new inbound client")
		}

		node.in_bound_connections = append(node.in_bound_connections, &new_inbound_client)

		// send new events (TODO (me): how much time does this func takes in ms)
		ingestion(states.camera_conn, states.kafka_conns, states.current_partition)

		// check on other servers
		for _, fn := range node.pingAll() {
			// TODO: set in pfail or fail
			log.Printf("This node did not send a PONG %s", fn)
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func main() {

	var node ClusterNode
	var cluster_state ClusterCronStates

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
