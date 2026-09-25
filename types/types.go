package types

// TODO : Rename later to common.go

const MAX_PACKET_SIZE = 1024
const MAX_DATA_SIZE = 512
const MAX_PARTITION = 3
const TDS_CONFIG = "/tmp/tds/"

type Packet struct {
	CameraID      uint64
	CameraAddress string
	Data          [MAX_DATA_SIZE]byte
}
