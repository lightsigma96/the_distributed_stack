package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func consume_event(conn *kafka.Conn, topic string, partition int) types.Packet {

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	batch := conn.ReadBatch(1, types.MAX_PACKET_SIZE) // fetch 1KB min, 1MB max

	defer batch.Close()

	p := types.Packet{}
	raw_bytes := make([]byte, types.MAX_PACKET_SIZE)
	n := 1
	var err error
	for n > 0 {
		n, err = batch.Read(raw_bytes)
		if err != nil {
			break
		}
	}

	p.CameraID = binary.BigEndian.Uint64(raw_bytes[0:8])

	cameraAddressLen := int(binary.BigEndian.Uint32(raw_bytes[8:12]))

	addressStart := 12
	addressEnd := addressStart + cameraAddressLen

	p.CameraAddress = string(raw_bytes[addressStart:addressEnd])

	cameraDataLen := int(binary.BigEndian.Uint32(
		raw_bytes[addressEnd : addressEnd+4],
	))

	dataStart := addressEnd + 4
	dataEnd := dataStart + cameraDataLen

	copy(p.Data[:], raw_bytes[dataStart:dataEnd])
	return p
}

func is_partition_empty(conn *kafka.Conn) bool {
	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		log.Fatal(err)
	}

	currentOffset, err := conn.Seek(0, 1)
	if err != nil {
		log.Fatal(err)
	}

	if currentOffset < lastOffset {
		fmt.Println("partition is not empty")
		return false
	}
	fmt.Println("partition is empty")
	return true
}

func main() {
	current_partition := 0
	const topic = "camera_event"

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	for {
		var packet types.Packet
		packet = consume_event(conn, topic, current_partition)

		time.Sleep(10 * (time.Second / 60))
		fmt.Printf("received and proceessed %d bytes from %s\n", len(packet.Data), packet.CameraAddress)

		if current_partition < types.MAX_PARTITION {
			current_partition += 1
		} else {
			current_partition = 0
		}
	}
}
