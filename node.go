package lamp

import (
	"crypto/sha1"
	"encoding/hex"
)

type Address struct {
	ID     int    `json:"id,omitempty"`
	Addr   string `json:"addr,omitempty"`
	Weight int    `json:"weight,omitempty"`
}

type Node struct {
	ID     int    `json:"id,omitempty"`
	Addr   string `json:"addr,omitempty"`
	Weight int    `json:"weight,omitempty"`
	Time   int64  `json:"time,omitempty"`
}

// generateNodeID
func generateNodeID(addr string) string {
	h := sha1.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}
