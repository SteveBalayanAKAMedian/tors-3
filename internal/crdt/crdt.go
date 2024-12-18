package crdt

import (
	//"crdt/internal/utils"

	"strings"
	"sync"
	"time"
)

type OperationType string

const (
	OpAdd    OperationType = "add"
	OpRemove OperationType = "remove"
)

type Operation struct {
	Key       string         `json:"key"`
	Value     string         `json:"value,omitempty"`
	Type      OperationType  `json:"type"`
	Timestamp map[string]int `json:"timestamp"`
	Origin    string         `json:"origin"`
}

type CRDT struct {
	data       map[string]string
	timestamps map[string]map[string]int
	lock       sync.RWMutex
	history    []Operation
	origin     string
	vector     map[string]int

	peers            []string
	heartbeatTimeout time.Duration
	heartbeatTimer   *time.Timer
}


func NewCRDT(origin string, peers []string) *CRDT {
	server := &CRDT{
		data:       make(map[string]string),
		timestamps: make(map[string]map[string]int),
		history:    []Operation{},
		origin:     origin,
		vector:     map[string]int{origin: 0},

		peers:            peers,
		heartbeatTimeout: time.Second * 10,
	}

	server.heartbeatTimer = time.AfterFunc(server.heartbeatTimeout, server.heartbeat)

	return server
}

func (c *CRDT) GetHistory() []Operation {
	return c.history
}

func (c *CRDT) GetTimestamps() map[string]map[string]int {
	return c.timestamps
}

func (c *CRDT) GetValue(key string) string {
	return c.data[key]
}

func (c *CRDT) GetData() map[string]string {
	return c.data
}

func (c *CRDT) incrementClock() {
	c.vector[c.origin]++
}

func (c *CRDT) mergeClock(peerVector map[string]int) {
	for replica, ts := range peerVector {
		if localTs, exists := c.vector[replica]; !exists || ts > localTs {
			c.vector[replica] = ts
		}
	}
}

func (c *CRDT) isLater(op Operation) bool {
	localTs, exists := c.timestamps[op.Key]
	if !exists {
		return true
	}

	isGreater := false
	isSmaller := false
	for replica, ts := range op.Timestamp {
		local, exists := localTs[replica]
		if !exists || ts > local {
			isGreater = true
		} else if ts < local {
			isSmaller = true
		}
	}

	for replica := range localTs {
		_, exists := op.Timestamp[replica]
		if !exists {
			isSmaller = true
			break
		}
	}

	if isGreater && !isSmaller {
		return true
	} else if isSmaller && !isGreater {
		return false
	} else if !isGreater && !isSmaller {
		return false
	}

	// tie-breaker
	return strings.Compare(op.Origin, c.origin) > 0
}

func (c *CRDT) apply(op Operation) {
	if c.isLater(op) {
		if op.Type == OpAdd {
			c.data[op.Key] = op.Value
		} else if op.Type == OpRemove {
			delete(c.data, op.Key)
		}
		if _, exists := c.timestamps[op.Key]; !exists {
			c.timestamps[op.Key] = make(map[string]int)
		}
		for replica, ts := range op.Timestamp {
			c.timestamps[op.Key][replica] = ts
		}
		c.history = append(c.history, op)
	}
}
