package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// import "sort"
// import "bufio"

// KeyValue Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// CreateReduceFile creates intermediate files
func CreateReduceFile(id int, nReduce int) ([]*os.File, []string, error) {
	files := make([]*os.File, nReduce)
	fileNames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-reduce-%d-%d", id, i)
		fileNames[i] = fileName
		f, err := os.Create(fileName)
		if err != nil {
			// try to remove the file
			err = os.Remove(fileName)
			if err != nil {
				log.Fatal("Failed to delete file", err)
			}
			f, err = os.Create(fileName)
			if err != nil {
				log.Fatal("Failed to create file", err)
			}
			//return files, fileNames, err
		}
		files[i] = f
	}
	return files, fileNames, nil
}

// ReadFile applies mapf for the file and do local reducef
func ReadFile(job Job, mapf func(string, string) []KeyValue) []KeyValue {
	FileName := job.FileName
	dat, err := ioutil.ReadFile(FileName)
	if err != nil {
		log.Fatal("Failed to open file", err)
	}

	intermediate := []KeyValue{}
	kvs := mapf(FileName, string(dat))
	intermediate = append(intermediate, kvs...)
	return intermediate
}

//Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	job, done := RequestJob()
	if done {
		return
	} else if job.JobType == MapJob {
		files, fileNames, err := CreateReduceFile(job.ID, job.NReduce)
		if nil != err {
			log.Fatal(err)
			return
		}
		intermediate := ReadFile(job, mapf)
		//fmt.Printf("%v+", intermediate)
		for _, v := range intermediate {
			id := ihash(v.Key) % job.NReduce
			file := files[id]
			// fmt.Printf("------%v %d", fileNames, len(fileNames))
			// fmt.Printf("%s", fmt.Sprintf("%s %s\n", v.Key, v.Value))
			n, err := file.WriteString(fmt.Sprintf("%s %s\n", v.Key, v.Value))
			if nil != err {
				panic(err)
			}
			log.Printf("%d Writed %d", job.ID, n)
		}
		for _, f := range files {
			defer f.Close()
		}
		FinishMapJob(job.ID, fileNames)
	} else if job.JobType == ReduceJob {

	} else {
		log.Fatal("Invalid job type")
	}
}

// FinishMapJob calls rpc FinishMapTask on master
func FinishMapJob(JobID JobID, FileNames []string) {
	args := MapJobFinishArgs{
		JobID: JobID,
		Intermediate: FileNames}
	reply := MapJobFinishReply{}
	if !call("Master.FinishMapTask", &args, &reply) {
		log.Fatal("Failed to call Master.FinishMapTask")
	}
}

// RequestJob function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestJob() (Job, bool) {

	// declare an argument structure.
	args := JobRequestArgs{}
	// fill in the argument(s).
	args.JobID = "-1"

	// declare a reply structure.
	reply := JobRequestReply{}
	wait := 1
	retry := 10
	// send the RPC request, wait for the reply.
	call("Master.GetTask", &args, &reply)

	for retry > 0 && reply.Retry {
		time.Sleep(time.Duration(wait) * time.Second)
		call("Master.GetTask", &args, &reply)
		wait *= 2
		retry--
	}
	if reply.Done {
		return reply.Job, true
	}
	// reply.Y should be 100.
	fmt.Printf("RequestJob %v\n", reply.Job)
	return reply.Job, false
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println(rpcname)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
