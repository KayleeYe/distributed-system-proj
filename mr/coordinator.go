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

type Coordinator struct {
	// Your definitions here.
	tchan      chan Task
	reducechan chan Task
	NewId      int
	MapWId     map[int]chan struct{}
	//MapWId  IdHolder
	MapLock sync.Mutex
	// done flag
	flag      bool
	flagLock  sync.Mutex
	counter   int
	nReduce   int
	waitGroup sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
/*func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
    reply.Y = args.X + 1
    return nil
}
*/

/*type IdHolder struct{
	MapWithId map[int]chan struct{}
}
*/
// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.flagLock.Lock()
	ret := c.flag
	c.flagLock.Unlock()
	//ret := false
	// Your code here.
	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	//Initialize coordinator
	c := Coordinator{

		tchan:      make(chan Task, len(files)),
		reducechan: make(chan Task, nReduce),
		NewId:      0,
		MapWId:     make(map[int]chan struct{}),
		//MapWId:    IdHolder{MapWithId: make(map[int]chan struct{})},
		flag:      false,
		flagLock:  sync.Mutex{},
		MapLock:   sync.Mutex{},
		nReduce:   nReduce,
		counter:   0,
		waitGroup: sync.WaitGroup{},
	}

	// send task to channel
	go func(files []string) {
		// send map task to rpc concurrently
		for _, filename := range files {
			c.waitGroup.Add(1)
			task := Task{
				Type:     Map,
				Filename: filename,
				TaskId:   c.Tid() - 1,
			}
			c.tchan <- task
			//c.taskQueue <- InitializeMapTask(filename, c.NewTaskId()-1)
		}
		//track the number of active tasks and wait for all map tasks to complete
		c.waitGroup.Wait()
		//wait all the c.waitGroup.Done() in finish Task Function

		for i := 0; i < nReduce; i++ {
			c.waitGroup.Add(1)
			task := Task{
				Type:     Reduce,
				TaskId:   c.Tid() - 1,
				ReduceId: i,
			}
			c.tchan <- task
		}
		// make Done() check goroutine
		// log.Println("waiting exit goroutine created")
		c.waitGroup.Wait()
		//c.Done()
		c.flagLock.Lock()
		c.flag = true
		c.flagLock.Unlock()

	}(files)
	c.server()
	return &c
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// grab a task from queue to rpc
	task := <-c.tchan

	reply.AssignT = task
	reply.NReduce = c.nReduce
	c.lock()
	// make task notification channel
	//IdHolder[task.TaskId] = make(chan struct{})
	c.MapWId[task.TaskId] = make(chan struct{})
	// call go func with timer
	c.monitorTask(c.MapWId[task.TaskId], task)
	c.unlock()

	return nil
}
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	taskId := args.TaskId
	NMap := c.MapWId
	if !c.notifyTaskCompletion(taskId) {
		return nil
	} else {
		delete(NMap, taskId)
		c.counter += 1
		c.unlock()
		return nil
	}
}

func (c *Coordinator) notifyTaskCompletion(taskId int) bool {
	c.lock()
	notifyChan, ok := c.MapWId[taskId]
	if !ok {
		return false
	} else {
		notifyChan <- struct{}{}
		c.waitGroup.Done()
		return true
	}
}

func (c *Coordinator) lock() {
	c.MapLock.Lock()
}

func (c *Coordinator) unlock() {
	c.MapLock.Unlock()
}

func (c *Coordinator) Tid() int {
	c.lock()
	defer c.unlock()
	c.NewId++
	return c.NewId
}

func (c *Coordinator) monitorTask(taskChan chan struct{}, task Task) {
	go func() {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop() // Clean up the timer if the task finishes before the timeout
		select {
		case <-taskChan:
			// Task is done.
			return
		case <-timer.C:
			// The task has timed out; put it back in the queue.
			c.tchan <- task
			return
		}
	}()
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
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
