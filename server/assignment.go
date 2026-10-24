package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

// ServerCandidate is one application server as seen by an assignment service.
// CameraCount is the most recently reported count of active camera endpoints.
type ServerCandidate struct {
	Address     string
	CameraCount int
	Healthy     bool
}

// selectServer scans all candidates and returns the healthy server with the
// fewest active cameras. Ties are resolved by input order. This is best-effort
// load balancing: reported counts can be stale, and no capacity is reserved.
func selectServer(candidates []ServerCandidate) (string, error) {
	bestIndex := -1

	for i, candidate := range candidates {
		if !candidate.Healthy || candidate.Address == "" || candidate.CameraCount < 0 {
			continue
		}
		if !validHostPort(candidate.Address) {
			continue
		}

		if bestIndex == -1 || candidate.CameraCount < candidates[bestIndex].CameraCount {
			bestIndex = i
		}
	}

	if bestIndex == -1 {
		return "", fmt.Errorf("no healthy application servers available")
	}

	return candidates[bestIndex].Address, nil
}

func assignmentCandidates(node *ClusterNode, localCameraAddress string, states *ClusterCronStates) []ServerCandidate {
	candidates := []ServerCandidate{{
		Address:     localCameraAddress,
		CameraCount: states.activeCameraCount(),
		Healthy:     true,
	}}

	node.mu.RLock()
	for _, peer := range node.connections {
		if peer.camera_addr == "" {
			continue
		}

		candidates = append(candidates, ServerCandidate{
			Address:     peer.camera_addr,
			CameraCount: peer.remote_camera_count,
			Healthy:     peer.healthy,
		})
	}
	node.mu.RUnlock()

	return candidates
}

func handleAssignment(conn net.Conn, node *ClusterNode, localCameraAddress string, states *ClusterCronStates) {
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		log.Printf("Could not set assignment connection deadline: %v", err)
		return
	}

	request, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Printf("Could not read assignment request: %v", err)
		return
	}
	if strings.TrimSpace(request) != "ASSIGN" {
		_, _ = fmt.Fprintln(conn, "ERR expected ASSIGN")
		return
	}

	address, err := selectServer(assignmentCandidates(node, localCameraAddress, states))
	if err != nil {
		_, _ = fmt.Fprintln(conn, "ERR no healthy application servers")
		return
	}

	if _, err := fmt.Fprintln(conn, address); err != nil {
		log.Printf("Could not return camera assignment: %v", err)
	}
}

// serveAssignment handles assignment requests sequentially. The handler only
// selects and returns an address; cameras connect to the selected server later.
func serveAssignment(listener net.Listener, node *ClusterNode, localCameraAddress string, states *ClusterCronStates) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "closed network connection") {
				return
			}
			log.Printf("Error accepting camera assignment request: %v", err)
			continue
		}

		handleAssignment(conn, node, localCameraAddress, states)
	}
}
