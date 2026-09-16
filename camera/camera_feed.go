package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"time"
)

const MAX_PACKET_SIZE = 1024

type Packet struct {
	ID   uint64
	Data [MAX_PACKET_SIZE]byte
}

func main() {
	conn, err := net.Dial("udp4", ":8080")

	if err != nil {
		log.Fatalln("COULD NOT START SERVER")
	}

	var counter uint64

	defer conn.Close()
	for {
		var p Packet
		p.ID = counter

		copy(p.Data[:2], "hi")

		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, p)

		_, err := conn.Write(buf.Bytes())
		if err != nil {
			log.Println("error sending packet:", err)
		}

		counter++
		time.Sleep(time.Millisecond * 10)
	}
}
