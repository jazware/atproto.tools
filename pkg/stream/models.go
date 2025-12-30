package stream

import (
	"time"
)

type Record struct {
	ID          int64
	CreatedAt   time.Time
	FirehoseSeq int64
	Repo        string
	Collection  string
	RKey        string
	Action      string
	Raw         []byte // Raw JSON data
}

type Event struct {
	CreatedAt   time.Time
	FirehoseSeq int64
	Repo        string
	EventType   string
	Error       string
	Time        int64
	Since       *string
}

type Cursor struct {
	ID        int64
	LastSeq   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Identity struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	DID       string
	Handle    string
	PDS       string
}
