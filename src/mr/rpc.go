package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.

// JobRequestArgs represents the request args 
type JobRequestArgs struct {
	JobID JobID
}
// MapJobFinishArgs represents the request args 
type MapJobFinishArgs struct {
	JobID JobID
	Intermediate []string
}

// MapJobFinishReply represents
type MapJobFinishReply struct {
	Done bool
}

// JobRequestReply represents the response 
type JobRequestReply struct {
	Done bool
	Retry bool
	Job Job 
	NReduce int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
