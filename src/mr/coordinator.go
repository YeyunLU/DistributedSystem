package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu         sync.Mutex
	nReduce    int
	mapIdx     int
	reduceIdx  int
	inputFiles []string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(previousTask TaskType, taskArgs *TaskArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Increase idx for map or reduce tasks that are already done
	if previousTask == 0 {
		c.mapIdx++
	} else if previousTask == 1 {
		c.reduceIdx++
	}

	// Assign different tasks to workers
	if c.mapIdx < len(c.inputFiles) {
		// Assigning map tasks
		println("Assigning Map task to the worker")
		taskArgs.InputFile = c.inputFiles[c.mapIdx]
		taskArgs.Type = Map
		taskArgs.Idx = c.mapIdx
		taskArgs.NReduce = c.nReduce
	} else if c.reduceIdx < c.nReduce {
		// Assigning reduce tasks
		println("Assigning Reduce task to the worker")
		taskArgs.Type = Reduce
		taskArgs.Idx = c.reduceIdx
		taskArgs.NMap = len(c.inputFiles)
	} else {
		// Terminate the workers
		println("Finished all the job, stop working")
		taskArgs.Type = None
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	e := rpc.Register(c)
	if e != nil {
		log.Fatal("register error:", e)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapIdx >= len(c.inputFiles) && c.reduceIdx >= c.nReduce 
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.inputFiles = files
	c.nReduce = nReduce

	c.server()
	return &c
}
