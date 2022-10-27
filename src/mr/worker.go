package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
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
	previousTaskType := None // TaskType.None
	for {
		taskArgs := TaskArgs{}
		err := askForTask(previousTaskType, &taskArgs) // use & pointer to update value for the input param
		
		// terminate if fail to communicate with the coordiator
		// the possible reason would be we already terminated the coordiator
		if err != nil {
			break
		}

		if taskArgs.Type == 0 { // Got a Map task
			executeMapTask(taskArgs, mapf)
			previousTaskType = Map
		} else if taskArgs.Type == 1 { // Got a Reduce task
			executeReduceTask(taskArgs, reducef)
			previousTaskType = Reduce
		} else { // Got no tasks, exit
			break
		}
		time.Sleep(time.Second) // sleep 1 s between 2 tasks
	}
}

func askForTask(previousTask TaskType,taskArgs *TaskArgs) error {
	ok := call("Coordinator.AssignTask", previousTask, &taskArgs)
	if !ok {
		log.Fatal("Failed to ask for a task")
	}
	return nil
}

func executeMapTask(taskArgs TaskArgs, mapf func(string, string) []KeyValue) error {

	fileName := taskArgs.InputFile
	mapTaskIdx := taskArgs.Idx
	fmt.Printf("Executing Map Task... Input File: %s \n", fileName)

	// Read content from the input file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	// Call map function from wc.go
	kva := mapf(fileName, string(content))

	sort.Sort(ByKey(kva))

	// Write to intermidiate files
	for _, kv := range kva {
		reduceIdx := ihash(kv.Key) % taskArgs.NReduce
		// example intermidate output files name: mr-0-0
		oname := "mr-" + strconv.Itoa(mapTaskIdx) + "-" + strconv.Itoa(reduceIdx)
		// Wirte to the file if exist, and create new files if not exist
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		ofile.Close()
	}

	return nil
}

func executeReduceTask(taskArgs TaskArgs, reducef func(string, []string) string) error {
	taskIdx := taskArgs.Idx
	nMapTask := taskArgs.NMap
	currMapIdx := 0
	fmt.Printf("Executing Reduce Task...\n")
	counts := map[string]int{}
	for currMapIdx < nMapTask {
		// Read input from intermidate files
		fileName := "mr-" + strconv.Itoa(currMapIdx) + "-" + strconv.Itoa(taskIdx)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()
		items := strings.Split(string(content), "\n")
		if len(items) <= 0 {
			fmt.Printf("No items in file: %s \n", fileName)
			return nil
		}
		// Reduce implementation
		curIdx := 0
		for curIdx < len(items) {
			tmpIdx := curIdx + 1
			if len(items[curIdx]) == 0 { // skip empty inputs
				curIdx++
				continue
			}
			kvItem := strings.Split(items[curIdx], " ") // kvItem[0] for key, kvItem[1] for value
			for tmpIdx < len(items) && items[tmpIdx] == items[curIdx] {
				tmpIdx++
			}
			values := []string{}
			for k := curIdx; k < tmpIdx; k++ {
				values = append(values, kvItem[1])
			}
			reduceValue, _ := strconv.Atoi(reducef(kvItem[0], values))
			counts[kvItem[0]] = counts[kvItem[0]] + reduceValue
			curIdx = tmpIdx
		}
		currMapIdx++
	}
	// Write results into output files
	oname := "mr-out-" + strconv.Itoa(taskIdx)
	ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		log.Fatal(err)
	}
	for key, value := range counts {
		fmt.Fprintf(ofile, "%v %v\n", key, strconv.Itoa(value))
	}
	ofile.Close()
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
