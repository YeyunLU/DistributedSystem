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
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// while true loop until there is no tasks left
	previousTask := PreviousTask{Type: None} // TaskType.None
	for {
		taskArgs := TaskArgs{}
		askForTask(previousTask, &taskArgs) // use & pointer to update value for the input param
		if taskArgs.Type == 0 { // Got a Map task
			executeMapTask(taskArgs, mapf)
			previousTask.Type = Map // Update to mark done
			previousTask.Idx = taskArgs.Idx
		} else if taskArgs.Type == 1 { // Got a Reduce task
			executeReduceTask(taskArgs, reducef)
			previousTask.Type = Reduce // Update to mark done
			previousTask.Idx = taskArgs.Idx
		} else { // Got no tasks, exit
			break
		}
		// time.Sleep(time.Second) // sleep 1 s between 2 tasks
	}
}

func askForTask(previousTask PreviousTask, taskArgs *TaskArgs) error {
	ok := call("Coordinator.AssignTask", previousTask, taskArgs)
	if !ok { // Assume that we already terminate the coordinator
		log.Fatal("Failed to ask for a task")
		os.Exit(0)
	}
	return nil
}

func executeMapTask(taskArgs TaskArgs, mapf func(string, string) []KeyValue) error {

	fileName := taskArgs.InputFile
	mapTaskIdx := taskArgs.Idx
	// fmt.Printf("Executing Map Task... Input File: %s \n", fileName)

	// Read content from the input file
	file, _ := os.Open(fileName)
	content, _ := ioutil.ReadAll(file)
	defer file.Close()

	// Call map function from wc.go
	kva := mapf(fileName, string(content))

	sort.Sort(ByKey(kva))

	// Group key value pairs by hash key
	kvsByReduce := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceIdx := ihash(kv.Key) % taskArgs.NReduce
		kvsByReduce[reduceIdx] = append(kvsByReduce[reduceIdx], kv)
	}

	// Write to intermidiate files
	for idx, kvs := range kvsByReduce {
		oname := "mr-" + strconv.Itoa(mapTaskIdx) + "-" + strconv.Itoa(idx)
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	return nil
}

func executeReduceTask(taskArgs TaskArgs, reducef func(string, []string) string) error {
	taskIdx := taskArgs.Idx
	nMapTask := taskArgs.NMap
	currMapIdx := 0
	// fmt.Printf("Executing Reduce Task %d...\n", taskIdx)
	kva := []KeyValue{} // local cache for all the key value pairs
	reduceValues := map[string]string{}
	for currMapIdx < nMapTask {
		// Read input from intermidate files
		fileName := "mr-" + strconv.Itoa(currMapIdx) + "-" + strconv.Itoa(taskIdx)
		file, _ := os.Open(fileName)
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		currMapIdx++
	}
	sort.Sort(ByKey(kva))
	// Reduce implementation
	curIdx := 0
	for curIdx < len(kva) {
		tmpIdx := curIdx + 1
		key := kva[curIdx].Key
		value := kva[curIdx].Value
		for tmpIdx < len(kva) && kva[tmpIdx].Key == key {
			tmpIdx++
		}
		values := []string{}
		for k := curIdx; k < tmpIdx; k++ {
			values = append(values, value)
		}
		reduceValue := reducef(key, values)
		reduceValues[key] = reduceValue
		curIdx = tmpIdx
	}
	// Write results into output files
	oname := "mr-out-" + strconv.Itoa(taskIdx)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for key, value := range reduceValues {
		fmt.Fprintf(ofile, "%v %v\n", key, value)
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
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
