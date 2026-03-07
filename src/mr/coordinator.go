package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	ID        int
	Type      TaskType
	State     TaskState
	FileNames []string
	NReduce   int
	StartTime time.Time
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	mu sync.Mutex

	inputFiles []string
	nMap       int
	nReduce    int

	phase Phase

	mapTasks    []Task
	reduceTasks []Task

	completedMaps    int
	completedReduces int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	const timeout = 10 * time.Second

	switch c.phase {
	case MapPhase:
		for i := range c.mapTasks {
			t := &c.mapTasks[i]
			if t.State == Idle || (t.State == InProgress && time.Since(t.StartTime) > timeout) {
				t.State = InProgress
				t.StartTime = time.Now()
				reply.Task = *t
				return nil
			}
		}
		reply.Task = Task{Type: NoTask}
		return nil

	case ReducePhase:
		for i := range c.reduceTasks {
			t := &c.reduceTasks[i]
			if t.State == Idle || (t.State == InProgress && time.Since(t.StartTime) > timeout) {
				t.State = InProgress
				t.StartTime = time.Now()
				reply.Task = *t
				return nil
			}
		}
		reply.Task = Task{Type: NoTask}
		return nil

	case DonePhase:
		reply.Task = Task{Type: ExitTask}
		return nil
	}

	reply.Task = Task{Type: NoTask}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskID < len(c.mapTasks) && c.mapTasks[args.TaskID].State == InProgress {
			c.mapTasks[args.TaskID].State = Completed
			c.completedMaps++
			log.Printf("Map task %d completed (%d/%d)", args.TaskID, c.completedMaps, c.nMap)
			if c.completedMaps == c.nMap {
				c.phase = ReducePhase
				log.Printf("All map tasks done, moving to reduce phase")
			}
		}
	case ReduceTask:
		if args.TaskID < len(c.reduceTasks) && c.reduceTasks[args.TaskID].State == InProgress {
			c.reduceTasks[args.TaskID].State = Completed
			c.completedReduces++
			log.Printf("Reduce task %d completed (%d/%d)", args.TaskID, c.completedReduces, c.nReduce)
			if c.completedReduces == c.nReduce {
				c.phase = DonePhase
				log.Printf("All reduce tasks done")
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:       files,
		nMap:             len(files),
		nReduce:          nReduce,
		phase:            MapPhase,
		mapTasks:         make([]Task, len(files)),
		reduceTasks:      make([]Task, nReduce),
		completedMaps:    0,
		completedReduces: 0,
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			ID:        i,
			Type:      MapTask,
			State:     Idle,
			FileNames: []string{file},
			NReduce:   nReduce,
			StartTime: time.Time{},
		}
	}

	for r := 0; r < nReduce; r++ {
		files := make([]string, len(c.inputFiles))
		for m := 0; m < len(c.inputFiles); m++ {
			files[m] = fmt.Sprintf("intermediate/mr-%d-%d", m, r)
		}
		c.reduceTasks[r] = Task{
			ID:        r,
			Type:      ReduceTask,
			State:     Idle,
			FileNames: files,
			NReduce:   nReduce,
		}
	}

	c.server(sockname)
	return &c
}
