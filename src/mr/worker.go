package mr

import (
    "fmt"
    "hash/fnv"
    "io/ioutil"
    "log"
    "net/rpc"
    "os"
    "strconv"
    "time"
)

//
// Map functions return a slice of KeyValue.
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) {

    // Your worker implementation here.

    // uncomment to send the Example RPC to the coordinator.
    // CallExample()

    for {
        taskArgs := TaskArgs{}
        askForTask(&taskArgs)
        if taskArgs.Type == 0 {
            executeMapTask(taskArgs, mapf)
        } else if taskArgs.Type == 1{
            executeReduceTask(taskArgs, reducef)
        } else {
            break;
        }
    }
}

func askForTask(taskArgs * TaskArgs) error {
    ok := call("Coordinator.AssignTask", "asking", &taskArgs)
    if !ok {
        fmt.Printf("Failed to ask for a task!\n")
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

    // Write to intermidiate files
    for _, kv := range kva {
        reduceIdx := ihash(kv.Key) % taskArgs.NReduce
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
    fileName := taskArgs.InputFile
    fmt.Printf("Executing Reduce Task... Input: %s \n", fileName)
    time.Sleep(time.Second)
    // TODO functionality
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
