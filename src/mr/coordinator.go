package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	idx       int
	inputFile string
	StartAt   time.Time
	Done      bool
}

type ReduceTask struct {
	idx     int
	StartAt time.Time
	Done    bool
}

type Coordinator struct {
	mu               sync.Mutex
	nMap             int
	nReduce          int
	mapTasks         []MapTask
	reduceTasks      []ReduceTask
	remainMapTask    int
	remainReduceTask int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(previousTask PreviousTask, taskArgs *TaskArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Increase idx for map or reduce tasks that are already Done
	if previousTask.Type == 0 {
		if !c.mapTasks[previousTask.Idx].Done {
			// fmt.Printf("Worker %d finished Map task %d at time: %s \n",
			// 	previousTask.WorkerIdx, previousTask.Idx, time.Now())
			c.mapTasks[previousTask.Idx].Done = true
			c.remainMapTask--
		}
	} else if previousTask.Type == 1 {
		if !c.reduceTasks[previousTask.Idx].Done {
			// fmt.Printf("Worker %d finished Reduce task %d at time: %s \n",
			// 	previousTask.WorkerIdx, previousTask.Idx, time.Now())
			c.reduceTasks[previousTask.Idx].Done = true
			c.remainReduceTask--
		}
	}

	// Assign different tasks to workers
	now := time.Now()
	tenMinsAgo := now.Add(-10 * time.Second)
	if c.remainMapTask > 0 {
		// Assigning map tasks
		for idx := range c.mapTasks {
			task := &c.mapTasks[idx]
			if !task.Done && task.StartAt.Before(tenMinsAgo) {
				// fmt.Printf("Assigning Map task %d to the worker %d at time: %s \n",
				// 	task.idx, previousTask.WorkerIdx, time.Now())
				task.StartAt = now
				taskArgs.InputFile = task.inputFile
				taskArgs.Type = Map
				taskArgs.Idx = task.idx
				taskArgs.NReduce = c.nReduce
				return nil
			}
		}
	} else if c.remainReduceTask > 0 {
		// Assigning reduce tasks
		for idx := range c.reduceTasks {
			task := &c.reduceTasks[idx]
			if !task.Done && task.StartAt.Before(tenMinsAgo) {
				// fmt.Printf("Assigning Reduce task %d to the worker %d at time: %s\n",
				// 	task.idx, previousTask.WorkerIdx, time.Now())
				task.StartAt = now
				taskArgs.Type = Reduce
				taskArgs.Idx = task.idx
				taskArgs.NMap = c.nMap
				return nil
			}
		}
	} else {
		// Terminate the workers
		// println("Finished all the job, stop working")
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
	return c.remainMapTask == 0 && c.remainReduceTask == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:             len(files),
		nReduce:          nReduce,
		mapTasks:         make([]MapTask, len(files)),
		reduceTasks:      make([]ReduceTask, nReduce),
		remainMapTask:    len(files),
		remainReduceTask: nReduce,
	}
	for idx, file := range files {
		c.mapTasks[idx] = MapTask{idx: idx, inputFile: file, Done: false}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{idx: i, Done: false}
	}

	c.server()
	return &c
}
