package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
/*type ExampleArgs struct {
    X int
}
type ExampleReply struct {
    Y int
}
*/

// Add your RPC definitions here.
// Task represents a single Map or Reduce task.
type Task struct {
	Type     string // task type including Map and Reduce
	Filename string
	TaskId   int // need it as to check with the map channel in coordinator
	ReduceId int // Reducerid needed for partition
}

const (
	Map    = "map"
	Reduce = "reduce"
	Wait   = "wait"
)

// define arg when worker send request to coordinator.
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	AssignT  Task   // The task assigned to the worker
	NReduce  int    //ensure the total number of reduce tasks to make the number of partitions
	Filename string //grab the file from coordinator and give a name
	TaskId   int
}

type FinishTaskArgs struct {
	TaskId int
}
type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
