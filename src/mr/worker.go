package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	//"time"
)

// import "sort"
// import "bufio"

// KeyValue Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// Debug param
const Debug bool = true

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Seperator
const (
	Seperator string = "\t"
)

// CreateReduceFile creates intermediate files
func CreateReduceFile(id JobID, nReduce int) ([]*os.File, []string, error) {
	files := make([]*os.File, nReduce)
	fileNames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-reduce-%s-%d", id, i)
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

	for job, done := RequestJob(); true; job, done = RequestJob() {
		if done {
			return
		}
		if Debug {
			fmt.Printf("get Job %v+", job)
		}

		if job.JobType == MapJob {
			files, fileNames, err := CreateReduceFile(job.ID, job.NReduce)
			if nil != err {
				log.Fatal(err)
				return
			}
			// time.Sleep(time.Duration(30) * time.Second)
			intermediate := ReadFile(job, mapf)
			sort.Sort(ByKey(intermediate))
			for _, v := range intermediate {
				id := ihash(v.Key) % job.NReduce
				file := files[id]
				// fmt.Printf("------%v %d", fileNames, len(fileNames))
				// fmt.Printf("%s", fmt.Sprintf("%s %s\n", v.Key, v.Value))
				_, err := file.WriteString(fmt.Sprintf("%s%s%s\n", v.Key, Seperator, v.Value))
				if nil != err {
					panic(err)
				}
				// log.Printf("%d %s Writed %d",i, job.ID, n)
			}
			for _, f := range files {
				defer f.Close()
			}
			// break
			FinishMapJob(job.ID, fileNames)
		} else if job.JobType == ReduceJob {
			if Debug {
				fmt.Printf("received reduce task %v+", job)
			}
			files := strings.Split(job.FileName, ",")
			oname := fmt.Sprintf("mr-out-%s", job.ID)
			ofile, _ := os.Create(oname)
			resultMap := make(map[string][]string, 0)
			for _, f := range files {
				lines, err := readFileWithScanner(f)
				if err != nil {
					panic(err)
				}
				i := 0
				for i < len(lines) {
					j := i + 1
					values := []string{lines[i].Value}
					for j < len(lines) && lines[j].Key == lines[i].Key {
						values = append(values, lines[j].Value)
						j++
					}
					// output := reducef(lines[i].Key, values)
					// this is the correct format for each line of Reduce output.
					li, exist := resultMap[lines[i].Key]
					if exist {
						resultMap[lines[i].Key] = append(li, values...)
					} else {
						resultMap[lines[i].Key] = values
					}
					i = j
				}
			}
			for k, v := range resultMap {
				fmt.Fprintf(ofile, "%v %v\n", k, reducef(k, v))
			}
			FinishReduceJob(job.ID)
		} else {
			log.Fatal("Invalid job type")
			break
		}
	}
}
func readFileWithScanner(fn string) ([]KeyValue, error) {
	file, err := os.Open(fn)
	if err != nil {
		return []KeyValue{}, err
	}
	defer file.Close()

	// Start reading from the file using a scanner.
	scanner := bufio.NewScanner(file)
	result := make([]KeyValue, 0)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, Seperator)
		result = append(result, KeyValue{Key: tokens[0], Value: tokens[1]})
	}
	if scanner.Err() != nil {
		fmt.Printf(" > Failed with error %v\n", scanner.Err())
		return result, scanner.Err()
	}
	return result, nil
}

// FinishMapJob calls rpc FinishMapTask on master
func FinishMapJob(JobID JobID, FileNames []string) {
	args := JobFinishArgs{
		JobID:        JobID,
		Intermediate: FileNames}
	reply := JobFinishReply{}
	if !call("Master.FinishMapTask", &args, &reply) {
		log.Fatal("Failed to call Master.FinishMapTask")
	}
}

// FinishReduceJob calls rpc FinishMapTask on master
func FinishReduceJob(JobID JobID) {
	args := JobFinishArgs{
		JobID: JobID}
	reply := JobFinishReply{}
	if !call("Master.FinishReduceTask", &args, &reply) {
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
	//wait := 1
	//retry := 10
	// send the RPC request, wait for the reply.
	call("Master.GetTask", &args, &reply)
	/*
		for retry > 0 && reply.Retry {
			time.Sleep(time.Duration(wait) * time.Second)
			call("Master.GetTask", &args, &reply)
			wait *= 2
			retry--
		}*/
	// reply.Y should be 100.
	if Debug {
		fmt.Printf("RequestJob %v\n", reply.Job)
	}
	return reply.Job, reply.Done
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
