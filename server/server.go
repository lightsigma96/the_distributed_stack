package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
)

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

func main() {
	const topic = "camera-topic"

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{
		Port: 8080,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

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

	for {
		buf := make([]byte, 1024)

		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("error receiving packet:", err)
			continue
		}

		raw := buf[:n]

		if len(raw) < 12 {
			log.Println("packet too small")
			continue
		}

		var p types.Packet

		p.CameraID = binary.BigEndian.Uint64(raw[0:8])

		addressLen := int(binary.BigEndian.Uint32(raw[8:12]))

		addressStart := 12
		addressEnd := addressStart + addressLen

		if addressEnd+4 > len(raw) {
			log.Println("invalid address length")
			continue
		}

		p.CameraAddress = string(raw[addressStart:addressEnd])

		dataLen := int(binary.BigEndian.Uint32(
			raw[addressEnd : addressEnd+4],
		))

		dataStart := addressEnd + 4
		dataEnd := dataStart + dataLen

		if dataEnd > len(raw) || dataLen > len(p.Data) {
			log.Println("invalid data length")
			continue
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
}
