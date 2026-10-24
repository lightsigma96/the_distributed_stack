package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/lightsigma96/the_distributed_stack/types"
)

func requestAssignment(bootstrapAddresses []string) (string, error) {
	var failures []string

	for _, address := range bootstrapAddresses {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}

		conn, err := net.DialTimeout("tcp4", address, time.Second)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", address, err))
			continue
		}

		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := fmt.Fprintln(conn, "ASSIGN"); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", address, err))
			conn.Close()
			continue
		}

		reply, readErr := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		if readErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", address, readErr))
			continue
		}

		reply = strings.TrimSpace(reply)
		if strings.HasPrefix(reply, "ERR ") {
			failures = append(failures, fmt.Sprintf("%s: %s", address, reply))
			continue
		}
		if _, _, err := net.SplitHostPort(reply); err != nil {
			failures = append(failures, fmt.Sprintf("%s: invalid assignment response %q", address, reply))
			continue
		}

		return reply, nil
	}

	if len(failures) == 0 {
		return "", fmt.Errorf("no assignment bootstrap addresses provided")
	}
	return "", fmt.Errorf("could not obtain a camera assignment: %s", strings.Join(failures, "; "))
}

func main() {
	// -assign_addr accepts a comma-separated list so the camera can fall back
	// to another assignment endpoint if one is unavailable.
	assignAddr := flag.String("assign_addr", "", "Comma-separated TCP assignment endpoints (for example localhost:7201,localhost:7202)")
	serverAddr := flag.String("server_addr", "", "Direct UDP server address (bypasses assignment; useful for testing)")
	flag.Parse()

	udpServerAddress := strings.TrimSpace(*serverAddr)
	if *assignAddr != "" {
		bootstrapAddresses := strings.Split(*assignAddr, ",")
		address, err := requestAssignment(bootstrapAddresses)
		if err != nil {
			log.Fatalf("Could not obtain camera assignment: %v", err)
		}
		udpServerAddress = address
	}
	if udpServerAddress == "" {
		log.Fatal("provide -assign_addr or -server_addr")
	}

	serverConn, err := net.Dial("udp4", udpServerAddress)
	if err != nil {
		log.Fatalf("Could not connect to assigned server %q: %v", udpServerAddress, err)
	}
	defer serverConn.Close()

	log.Printf("Sending camera feed to %s", udpServerAddress)

	var counter uint64
	if err := serverConn.SetReadDeadline(time.Now().Add((time.Second / 60) * 10)); err != nil {
		log.Println("Could not set deadline on read:", err)
	}

	for {
		var packet types.Packet
		packet.CameraID = counter
		packet.CameraAddress = serverConn.LocalAddr().String()
		copy(packet.Data[:2], "hi")

		var sendEvent []byte
		idSlice := make([]byte, 8)
		binary.BigEndian.PutUint64(idSlice, packet.CameraID)
		sendEvent = append(sendEvent, idSlice...)

		sendEvent = binary.BigEndian.AppendUint32(
			sendEvent,
			uint32(len(packet.CameraAddress)),
		)
		sendEvent = append(sendEvent, packet.CameraAddress...)

		sendEvent = binary.BigEndian.AppendUint32(
			sendEvent,
			uint32(len(packet.Data)),
		)
		sendEvent = append(sendEvent, packet.Data[:]...)

		if _, err := serverConn.Write(sendEvent); err != nil {
			log.Println("write error:", err)
		}
		counter++
		time.Sleep(time.Second / 60)
	}
}
