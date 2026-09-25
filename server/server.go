package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"time"
)

func produce_event(conn *kafka.Conn, topic string, partition int, event types.Packet) {

	err := conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

	if err != nil {
		log.Printf("Error Setting Timeout on kafka write")
	}

	var send_event []byte

	// replace this wire protcol later with protobuf
	id_slice := make([]byte, 8)

	binary.BigEndian.PutUint64(id_slice, event.CameraID)
	send_event = append(send_event, id_slice...)

	binary.BigEndian.AppendUint32(send_event, uint32(len(event.CameraAddress)))
	send_event = append(send_event, event.CameraAddress...)

	binary.BigEndian.AppendUint32(send_event, uint32(len(event.Data)))
	send_event = append(send_event, event.Data[:]...)

	fmt.Printf("Partition: %d\n", partition)
	_, err = conn.WriteMessages(
		kafka.Message{Partition: partition, Value: send_event},
	)

	if err != nil {
		log.Printf("Error Queuing Packet, Packet Lost")
	}
}

func main() {

	current_partition := 0
	const topic = "camera_event"

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{
		Port: 8080,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	const MAX_ROUTINES = 3

	kafka_conn, kerr := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0) // no timeout on this connection

	defer kafka_conn.Close()

	if kerr != nil {
		log.Printf("Error Connecting Kafka Server, Packet Lost")
	}
	for {
		buf := make([]byte, 1024)

		_, _, err := conn.ReadFromUDP(buf)

		var p types.Packet
		binary.Read(
			bytes.NewReader(buf),
			binary.BigEndian,
			&p,
		)

		if err != nil {
			log.Println("error receiving packet:", err)
			continue
		}

		if current_partition < types.MAX_PARTITION {
			produce_event(kafka_conn, topic, current_partition, p)
			current_partition += 1
		} else {
			current_partition = 0
		}
	}
}
