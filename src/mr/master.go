package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
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
	// Timeout parameter
	Timeout int = 20
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
	Type      MsgType
	JobID     JobID
	Timestamp int
}

// Master contains waiting queue
type Master struct {
	// Your definitions here.
	NReduce        int
	NMap           int
	Panding        chan Job
	MsgQueue       chan UpdateMsg
	StartTimeTable map[JobID]int
	EndTimeTable   map[JobID]int
	ReduceFiles    map[int][]string
	mu             sync.Mutex
	Done           chan bool
	JobInfo        map[JobID]Job
}

// GetTask Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *JobRequestArgs, reply *JobRequestReply) error {
	j, more := <-m.Panding
	if more {
		t := int(time.Now().Unix())
		j.StartTimstamp = t
		reply.Done = false
		reply.Job = j
		reply.NReduce = m.NReduce

		msg := UpdateMsg{Type: JobStart, Timestamp: t, JobID: j.ID}
		m.MsgQueue <- msg
		// fmt.Println("received job", j)
	} else {
		reply.Done = true
		// fmt.Println("received all jobs")
	}
	return nil
}

// FinishMapTask should be call when worker has finished the task
func (m *Master) FinishMapTask(args *JobFinishArgs, reply *JobFinishReply) error {
	reply.Done = true
	// store intermediate files
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.StartTimeTable[args.JobID] > args.Starttime {
		// duplicated job, just discord
		return nil
	}
	for i, f := range args.Intermediate {
		m.ReduceFiles[i] = append(m.ReduceFiles[i], f)
	}

	msg := UpdateMsg{Type: JobFinish, Timestamp: int(time.Now().Unix()), JobID: args.JobID}
	m.MsgQueue <- msg
	return nil
}

// FinishReduceTask should be call when worker has finished the task
func (m *Master) FinishReduceTask(args *JobFinishArgs, reply *JobFinishReply) error {
	reply.Done = true
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.StartTimeTable[args.JobID] > args.Starttime {
		// duplicated job, just discord
		return nil
	}
	msg := UpdateMsg{Type: JobFinish, Timestamp: int(time.Now().Unix()), JobID: args.JobID}
	m.MsgQueue <- msg
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go m.monitor()
}

// checkTimeout
func (m *Master) checkTimeout() {
	now := int(time.Now().Unix())
	m.mu.Lock()
	defer m.mu.Unlock()
	for jobID, starttime := range m.StartTimeTable {
		if starttime != -1 && now-starttime > Timeout {
			fmt.Println("Timeout")
			m.StartTimeTable[jobID] = -1
			m.Panding <- m.JobInfo[jobID]
			return
		}
	}
}
func (m *Master) monitor() {
	for len(m.EndTimeTable) < m.NMap {
		m.checkTimeout()
		select {
		case msg := <-m.MsgQueue:
			if msg.Type == JobStart {
				//if _, exist := m.StartTimeTable[msg.JobID]; !exist {
				m.mu.Lock()
				m.StartTimeTable[msg.JobID] = msg.Timestamp
				m.mu.Unlock()
				//}
			} else {
				if _, exist := m.EndTimeTable[msg.JobID]; !exist {
					m.mu.Lock()
					m.EndTimeTable[msg.JobID] = msg.Timestamp
					m.mu.Unlock()
				}
			}
		default:
		}
	}
	if Debug {
		fmt.Println(m.StartTimeTable)
		fmt.Println(m.EndTimeTable)
	}

	// clear time table for reduce task
	m.mu.Lock()
	m.StartTimeTable = make(map[JobID]int)
	m.EndTimeTable = make(map[JobID]int)
	// add reduce tasks into queue
	for k, v := range m.ReduceFiles {
		id := JobID(strconv.Itoa(k))
		job := Job{ID: id, JobType: ReduceJob, FileName: strings.Join(v, ","), StartTimstamp: -1}
		m.JobInfo[id] = job
		m.Panding <- job
	}
	m.mu.Unlock()

	for len(m.EndTimeTable) < m.NReduce {
		m.checkTimeout()
		select {
		case msg := <-m.MsgQueue:
			if msg.Type == JobStart {
				//if _, exist := m.StartTimeTable[msg.JobID]; !exist {
				m.mu.Lock()
				m.StartTimeTable[msg.JobID] = msg.Timestamp
				m.mu.Unlock()
				//}
			} else {
				if _, exist := m.EndTimeTable[msg.JobID]; !exist {
					m.EndTimeTable[msg.JobID] = msg.Timestamp
				}
			}
		default:
		}

	}
	close(m.Panding) // close channel so get task will return done
	m.Done <- true   // send flag to isDone
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
		MsgQueue:       make(chan UpdateMsg, 100), // message queue
		NMap:           len(files),
		NReduce:        nReduce,
		Panding:        make(chan Job, 1000),
		StartTimeTable: make(map[JobID]int),
		EndTimeTable:   make(map[JobID]int),
		ReduceFiles:    make(map[int][]string),
		Done:           make(chan bool, 1),
		JobInfo:        make(map[JobID]Job)}

	for i := 0; i < nReduce; i++ {
		m.ReduceFiles[i] = make([]string, 0)
	}
	// Your code here.
	for _, s := range files {
		reader, err := os.Open(s)
		id := filepath.Base(s)
		if nil != err {
			log.Fatal(err)
		}
		defer reader.Close()
		job := Job{JobType: MapJob, ID: JobID(id), FileName: s, StartTimstamp: -1, NReduce: nReduce}
		m.JobInfo[JobID(id)] = job
		m.Panding <- job
	}
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
