package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
	None
)

type PreviousTask struct {
	Idx  int
	Type TaskType
}

type TaskArgs struct {
	InputFile string
	Type      TaskType
	Idx       int
	NMap      int
	NReduce   int
	StartAt   time.Time
	Done      bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	// sample output: s = /var/tmp/824-mr-501
	return s
}
