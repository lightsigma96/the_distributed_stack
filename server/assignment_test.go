package main

import "testing"

func TestSelectServerChoosesLowestLoad(t *testing.T) {
	candidates := []ServerCandidate{
		{Address: "127.0.0.1:7101", CameraCount: 8, Healthy: true},
		{Address: "127.0.0.1:7102", CameraCount: 3, Healthy: true},
		{Address: "127.0.0.1:7103", CameraCount: 5, Healthy: true},
	}

	got, err := selectServer(candidates)
	if err != nil {
		t.Fatalf("selectServer returned an error: %v", err)
	}
	if want := "127.0.0.1:7102"; got != want {
		t.Fatalf("selectServer() = %q, want %q", got, want)
	}
}

func TestSelectServerSkipsUnhealthyAndInvalidCandidates(t *testing.T) {
	candidates := []ServerCandidate{
		{Address: "127.0.0.1:7101", CameraCount: 0, Healthy: false},
		{Address: "not-an-address", CameraCount: 1, Healthy: true},
		{Address: "127.0.0.1:7102", CameraCount: 4, Healthy: true},
	}

	got, err := selectServer(candidates)
	if err != nil {
		t.Fatalf("selectServer returned an error: %v", err)
	}
	if want := "127.0.0.1:7102"; got != want {
		t.Fatalf("selectServer() = %q, want %q", got, want)
	}
}

func TestSelectServerErrorsWhenNoCandidateIsHealthy(t *testing.T) {
	candidates := []ServerCandidate{
		{Address: "127.0.0.1:7101", CameraCount: 0, Healthy: false},
		{Address: "127.0.0.1:7102", CameraCount: 1, Healthy: false},
	}

	if _, err := selectServer(candidates); err == nil {
		t.Fatal("selectServer() succeeded, want an error")
	}
}

func TestSelectServerKeepsFirstCandidateOnTie(t *testing.T) {
	candidates := []ServerCandidate{
		{Address: "127.0.0.1:7101", CameraCount: 2, Healthy: true},
		{Address: "127.0.0.1:7102", CameraCount: 2, Healthy: true},
	}

	got, err := selectServer(candidates)
	if err != nil {
		t.Fatalf("selectServer returned an error: %v", err)
	}
	if want := "127.0.0.1:7101"; got != want {
		t.Fatalf("selectServer() = %q, want %q", got, want)
	}
}
