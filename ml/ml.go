package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lightsigma96/the_distributed_stack/types"
	"github.com/segmentio/kafka-go"
)

func consume_event(reader *kafka.Reader) (types.Packet, error) {
	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		return types.Packet{}, err
	}
	fmt.Printf("\nmessage at topic/partition/offset %v/%v\n", msg.Topic, msg.Partition)
	raw_bytes := msg.Value

	if len(raw_bytes) < 12 {
		return types.Packet{}, fmt.Errorf("packet too small: %d bytes", len(raw_bytes))
	}

	p := types.Packet{}

	// Camera ID
	p.CameraID = binary.BigEndian.Uint64(raw_bytes[0:8])

	// Camera address
	cameraAddressLen := int(binary.BigEndian.Uint32(raw_bytes[8:12]))

	addressStart := 12
	addressEnd := addressStart + cameraAddressLen

	if addressEnd > len(raw_bytes) {
		return types.Packet{}, fmt.Errorf("invalid camera address length")
	}

	p.CameraAddress = string(raw_bytes[addressStart:addressEnd])

	// Camera data
	if addressEnd+4 > len(raw_bytes) {
		return types.Packet{}, fmt.Errorf("missing data length")
	}

	cameraDataLen := int(binary.BigEndian.Uint32(
		raw_bytes[addressEnd : addressEnd+4],
	))

	dataStart := addressEnd + 4
	dataEnd := dataStart + cameraDataLen

	if dataEnd > len(raw_bytes) {
		return types.Packet{}, fmt.Errorf("invalid camera data length")
	}

	if cameraDataLen > len(p.Data) {
		return types.Packet{}, fmt.Errorf(
			"camera data too large: %d bytes",
			cameraDataLen,
		)
	}

	copy(p.Data[:], raw_bytes[dataStart:dataEnd])

	return p, nil
}

func read_consumer_group_id() string {
	filename := fmt.Sprintf("%s/consumer_group_id", types.TDS_CONFIG)

	file_content, err := os.ReadFile(filename)
	if err != nil {
		log.Println("Could not read consumer group id, Aborting")
		return ""
	}

	return strings.TrimSpace(string(file_content))
}

func main() {
	const topic = "camera-topic"

	id := read_consumer_group_id()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  id,
		Topic:    topic,
		MaxBytes: 10e6,
	})

	defer r.Close()

	for {
		packet, err := consume_event(r)
		if err != nil {
			log.Println("error consuming packet:", err)
			continue
		}

		time.Sleep(10 * (time.Second / 60))

		fmt.Printf(
			"received and processed %d bytes from %s\n",
			len(packet.Data),
			packet.CameraAddress,
		)
	}
}
