package main

import (
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

		var send_event []byte

		id_slice := make([]byte, 8)
		binary.BigEndian.PutUint64(id_slice, p.CameraID)
		send_event = append(send_event, id_slice...)

		send_event = binary.BigEndian.AppendUint32(
			send_event,
			uint32(len(p.CameraAddress)),
		)
		send_event = append(send_event, p.CameraAddress...)

		send_event = binary.BigEndian.AppendUint32(
			send_event,
			uint32(len(p.Data)),
		)
		send_event = append(send_event, p.Data[:]...)

		_, err := server_conn.Write(send_event)
		if err != nil {
			log.Println("write error:", err)
		}
		counter++
		time.Sleep(time.Second / 60)

	}

}
