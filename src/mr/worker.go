package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string

func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &GetTaskArgs{}, &reply)
		if !ok {
			return
		}

		task := reply.Task

		switch task.Type {
		case MapTask:
			doMap(task, mapf)
			reportTask(task.ID, MapTask)

		case ReduceTask:
			doReduce(task, reducef)
			reportTask(task.ID, ReduceTask)

		case NoTask:
			time.Sleep(time.Second)

		case ExitTask:
			return

		default:
			return
		}
	}
}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	for _, filename := range task.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	os.MkdirAll("intermediate", 0755)

	for reduceID, kvs := range buckets {
		tmpFile, err := os.CreateTemp("intermediate", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}

		enc := json.NewEncoder(tmpFile)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode kv: %v", err)
			}
		}
		tmpFile.Close()

		finalName := fmt.Sprintf("intermediate/mr-%d-%d", task.ID, reduceID)
		if err := os.Rename(tmpFile.Name(), finalName); err != nil {
			log.Fatalf("cannot rename %v to %v: %v", tmpFile.Name(), finalName, err)
		}
	}
}

func doReduce(task Task, reducef func(string, []string) string) {
	var kvs []KeyValue
	for _, filename := range task.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v: %v", filename, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	tmpFile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = kvs[k].Value
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", kvs[i].Key, output)
		i = j
	}

	tmpFile.Close()
	finalName := fmt.Sprintf("mr-out-%d", task.ID)
	if err := os.Rename(tmpFile.Name(), finalName); err != nil {
		log.Fatalf("cannot rename %v to %v: %v", tmpFile.Name(), finalName, err)
	}
}

func reportTask(taskID int, taskType TaskType) {
	args := ReportTaskArgs{TaskID: taskID, TaskType: taskType}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
