package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{
		Port: 8080,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for {
		buf := make([]byte, 1024)

		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("error receiving packet:", err)
			continue
		}

		fmt.Printf("received %d bytes from %s\n", n, addr)
	}
}
