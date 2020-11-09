package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
	"strings"
	"strconv"
)
// import "io"
// import "bytes"

// const mappingJobLine int = 100000

// JobType is enum of job type
type JobType int
type JobID string
type MsgType int
const (
	// MapJob map
	MapJob JobType = 0
	// ReduceJob reduce
	ReduceJob JobType = 1
	// JobStart job start msg
	JobStart MsgType = 2
	// JobFinish job finished msg
	JobFinish MsgType = 3
)


// Job contains job information
type Job struct {
	ID            JobID
	FileName      string
	StartTimstamp int
	JobType       JobType
	NReduce       int
}
// UpdateMsg update message on time table
type UpdateMsg struct {
	Type MsgType
	JobID JobID
	Timestamp int
}
// Master contains waiting queue
type Master struct {
	// Your definitions here.
	NReduce  int
	NMap int
	Panding  chan Job
	MsgQueue chan UpdateMsg
	StartTimeTable  map[JobID]int
	EndTimeTable map[JobID] int
	ReduceFiles map[int] []string
	Done chan bool
}

// GetTask Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) GetTask(args *JobRequestArgs, reply *JobRequestReply) error {
	j, more := <-m.Panding
	if more {
		t:= int(time.Now().Unix())
		j.StartTimstamp = t
		reply.Done = false
		reply.Job = j
		reply.NReduce = m.NReduce

		msg := UpdateMsg {Type: JobStart, Timestamp:t, JobID: j.ID}
		m.MsgQueue <- msg
		// fmt.Println("received job", j)
	} else {
		reply.Done = false
		fmt.Println("received all jobs")
	}
	return nil
}

// FinishMapTask should be call when worker has finished the task
func (m *Master) FinishMapTask(args *MapJobFinishArgs, reply *MapJobFinishReply) error {
	reply.Done = true
	// store intermediate files
	for i, f:= range(args.Intermediate) {
		m.ReduceFiles[i] = append(m.ReduceFiles[i], f)
	}
	msg := UpdateMsg {Type: JobFinish, Timestamp:int(time.Now().Unix()), JobID: args.JobID}
	m.MsgQueue <- msg
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) monitor() {
	for len(m.EndTimeTable) < m.NMap {
		select {
		case msg:= <-m.MsgQueue:
			if msg.Type == JobStart{
				if _, exist:=m.StartTimeTable[msg.JobID]; !exist {
					m.StartTimeTable[msg.JobID] = msg.Timestamp
				}
			}else {
				if _, exist:=m.EndTimeTable[msg.JobID]; !exist {
					m.EndTimeTable[msg.JobID] = msg.Timestamp
				}
			}
		}
	}
	// clear time table for reduce task
	m.StartTimeTable = make(map[JobID]int)
	m.EndTimeTable = make(map[JobID]int)
	// add reduce tasks into queue
	for k, v:= range(m.ReduceFiles) {
		job:=Job{ID: JobID(strconv.Itoa(k)), JobType: ReduceJob, FileName: strings.Join(v, ","), StartTimstamp: -1}
		m.Panding <- job
	}
	for len(m.EndTimeTable) < m.NReduce {
		select {
		case msg:= <-m.MsgQueue:
			if msg.Type == JobStart{
				if _, exist:=m.StartTimeTable[msg.JobID]; !exist {
					m.StartTimeTable[msg.JobID] = msg.Timestamp
				}
			}else {
				if _, exist:=m.EndTimeTable[msg.JobID]; !exist {
					m.EndTimeTable[msg.JobID] = msg.Timestamp
				}
			}
		}
	}
	m.Done <- true
}

// IsDone main/mrmaster.go calls IsDone() periodically to find out
// if the entire job has finished.
//
func (m *Master) IsDone() bool {
	select {
    case msg := <-m.Done:
        return msg
    default:
        return false
    }
}

// MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	if nReduce <= 0 {
		panic("Invalid nReduce")
	}

	m := Master{
		MsgQueue: make(chan UpdateMsg, 100), // message queue
		NMap: len(files),
		NReduce:  nReduce,
		Panding:  make(chan Job, 1000),
		StartTimeTable: make(map[JobID] int),
		EndTimeTable: make(map[JobID] int),
		ReduceFiles: make(map[int][]string),
		Done: make(chan bool, 1)}
	
	for i := 0; i < nReduce; i ++ {
		m.ReduceFiles[i] = make([]string, 0)
	}
	// Your code here.
	for _, s := range files {
		reader, err := os.Open(s)
		defer reader.Close()
		if nil != err {
			log.Fatal(err)
		}
		job := Job{JobType: MapJob,ID: JobID(s), FileName: s, StartTimstamp: -1, NReduce: nReduce}
		m.Panding <- job
	}
	close(m.Panding)
	m.server()
	return &m
}

/*
func getFileSize(f io.Reader)(int) {
	fi, err := f.Stat()
	if err != nil {
	// Could not obtain stat, handle error
	}
	fmt.Printf("The file is %d bytes long", fi.Size())
}*/

/*
func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}
	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)
		switch {
		case err == io.EOF:
			return count, nil
		case err != nil:
			return count, err
		}
	}
}*/
