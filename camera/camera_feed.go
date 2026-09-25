package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"time"

	"github.com/lightsigma96/the_distributed_stack/types"
)

func main() {
	server_conn, err := net.Dial("udp4", "127.0.0.1:8080")

	if err != nil {
		log.Fatalln("COULD NOT START SERVER")
	}

	var counter uint64
	err = server_conn.SetReadDeadline(time.Now().Add((time.Second / 60) * 10))

	if err != nil {
		log.Println("Could not set deadline on read")
	}

	defer server_conn.Close()

	for {
		var p types.Packet
		p.CameraID = counter
		p.CameraAddress = server_conn.LocalAddr().String()

		copy(p.Data[:2], "hi")

		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, p)

		_, err := server_conn.Write(buf.Bytes())

		if err != nil {
			log.Println("write error")
		}
		counter++
		time.Sleep(time.Second / 60)

	}

}
