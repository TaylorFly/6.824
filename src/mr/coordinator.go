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

const TASK_TIMEOUT_MILLISECONDS = 10000

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	currentPhase CurrentPhase
	sourceFiles  []string
	reduceCnt    int
	mapCnt       int

	runningMapCnt     int
	finishedMapCnt    int
	runningReduceCnt  int
	finishedReduceCnt int
	allDone           bool

	mapTaskChan     chan *Task
	mapTaskState    []TaskState
	reduceTaskChan  chan *Task
	reduceTaskState []TaskState

	cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) produceMapTask() {

	pushNewTask := func(ID int) {
		task := Task{
			ID:        ID,
			FileName:  c.sourceFiles[ID],
			Phase:     MapPhase,
			ReduceCnt: c.reduceCnt,
		}
		c.mapTaskState[ID] = TaskState{
			State: Ready,
			Task:  &task,
		}
		c.mapTaskChan <- &task
	}

	// Map Task 生产
	c.mu.Lock()
	for i := 0; i < c.mapCnt; i++ {
		pushNewTask(i)
	}
	c.mu.Unlock()
	c.cond.Broadcast()

	isMap := func() bool {
		c.mu.Lock()
		ret := c.currentPhase == MapPhase
		c.mu.Unlock()
		return ret
	}

	for {
		time.Sleep(TASK_TIMEOUT_MILLISECONDS * time.Millisecond)
		if !isMap() {
			break
		}
		c.mu.Lock()
		for i := 0; i < c.mapCnt; i++ {
			// 正在运行且超时
			if c.mapTaskState[i].State == Running && time.Since(c.mapTaskState[i].startTime) > TASK_TIMEOUT_MILLISECONDS*time.Millisecond {
				c.mapTaskState[i].State = Ready
				c.runningMapCnt -= 1
				c.mapTaskChan <- c.mapTaskState[i].Task
				log.Println("Map Task Timeout: ", i)
				log.Println("Map Finish Count: ", c.finishedMapCnt)
				log.Println("Map Running Count: ", c.finishedMapCnt)
			}
		}
		c.mu.Unlock()
		c.cond.Broadcast()
	}
}

func (c *Coordinator) produceReduceTask() {
	pushNewTask := func(ID int) {
		task := Task{
			ID:        ID,
			Phase:     ReducePhase,
			ReduceCnt: c.reduceCnt,
		}
		c.reduceTaskState[ID] = TaskState{
			State: Ready,
			Task:  &task,
		}
		c.reduceTaskChan <- &task
	}

	// Reduce Task 生产
	c.mu.Lock()
	for i := 0; i < c.reduceCnt; i++ {
		pushNewTask(i)
	}
	c.mu.Unlock()
	c.cond.Broadcast()

	isReduce := func() bool {
		c.mu.Lock()
		ret := c.currentPhase == ReducePhase
		c.mu.Unlock()
		return ret
	}

	for {
		time.Sleep(TASK_TIMEOUT_MILLISECONDS * time.Millisecond)
		if !isReduce() {
			// 这里不能break,否则在map就退出
			continue
		}
		c.mu.Lock()
		for i := 0; i < c.reduceCnt; i++ {
			// 正在运行且超时
			if c.reduceTaskState[i].State == Running && time.Since(c.reduceTaskState[i].startTime) > TASK_TIMEOUT_MILLISECONDS*time.Millisecond {
				c.reduceTaskState[i].State = Ready
				c.runningReduceCnt -= 1
				c.reduceTaskChan <- c.reduceTaskState[i].Task
				log.Println("Reduce Task Timeout: ", i)
				log.Println("Finish Reduce Count: ", c.finishedReduceCnt)
				log.Println("Finish Reduce Running: ", c.runningReduceCnt)
			}
		}
		c.mu.Unlock()
		c.cond.Broadcast()
	}
}

func (c *Coordinator) PoolTask(args, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentPhase == MapPhase {
		if c.finishedMapCnt == c.mapCnt {
			c.currentPhase = ReducePhase
		} else {
			// 可能在未来全部执行完成，但是当前没有
			for c.runningMapCnt > 0 && c.runningMapCnt+c.finishedMapCnt == c.mapCnt {
				c.cond.Wait()
			}
			if c.finishedMapCnt < c.mapCnt {
				task := <-c.mapTaskChan
				c.mapTaskState[task.ID].State = Running
				c.mapTaskState[task.ID].startTime = time.Now()
				c.runningMapCnt++
				*reply = *task
			} else {
				c.currentPhase = ReducePhase
			}
		}
	}

	if c.currentPhase == ReducePhase {
		if c.finishedReduceCnt == c.reduceCnt {
			c.currentPhase = ExitPhase
		} else {
			for c.runningReduceCnt > 0 && c.runningReduceCnt+c.finishedReduceCnt == c.reduceCnt {
				c.cond.Wait()
			}
			if c.finishedReduceCnt < c.reduceCnt {
				task := <-c.reduceTaskChan
				c.reduceTaskState[task.ID].State = Running
				c.reduceTaskState[task.ID].startTime = time.Now()
				c.runningReduceCnt++
				*reply = *task
			} else {
				c.currentPhase = ExitPhase
			}
		}
	}

	if c.currentPhase == ExitPhase {
		task := Task{Phase: ExitPhase}
		*reply = task
		c.allDone = true
	}

	log.Printf("Finish Map Cnt: %d, Finish Reduce Cnt: %d", c.finishedMapCnt, c.finishedReduceCnt)

	running_cnt := 0
	finish_cnt := 0
	for i := 0; i < c.mapCnt; i++ {
		switch c.mapTaskState[i].State {
		case Running:
			running_cnt++
		case Finish:
			finish_cnt++
		}
	}
	if running_cnt != c.runningMapCnt {
		log.Fatal("Running Cnt", running_cnt, c.runningMapCnt)
	}

	if finish_cnt != c.finishedMapCnt {
		log.Fatal("finish Cnt", finish_cnt, c.finishedMapCnt)
	}

	if c.mapCnt-len(c.mapTaskChan) != c.runningMapCnt+c.finishedMapCnt {
		log.Fatal("task chan len error")
	}

	return nil
}

// 完成一个Task
func (c *Coordinator) CallDone(args, reply *Task) error {
	c.mu.Lock()
	ID := args.ID

	if c.currentPhase == MapPhase {

		// 如果判断超时了，但是这里执行成功了，我们不管，当他失败，重新执行
		if c.mapTaskState[ID].State == Ready {
			c.mu.Unlock()
			c.cond.Broadcast()
			return nil
		}

		c.mapTaskState[ID].State = Finish
		c.runningMapCnt -= 1
		c.finishedMapCnt += 1

		if c.finishedMapCnt == c.mapCnt {
			c.currentPhase = ReducePhase
		}
		c.mu.Unlock()
		c.cond.Broadcast()
	} else if c.currentPhase == ReducePhase {

		if c.reduceTaskState[ID].State == Ready {
			c.mu.Unlock()
			c.cond.Broadcast()
			return nil
		}

		c.reduceTaskState[ID].State = Finish
		c.runningReduceCnt -= 1
		c.finishedReduceCnt += 1

		if c.finishedReduceCnt == c.reduceCnt {
			c.currentPhase = ExitPhase
			c.allDone = true
		}
		c.mu.Unlock()
		c.cond.Broadcast()
	} else {
		// 不要忘了这个 unlock
		c.mu.Unlock()
		c.cond.Broadcast()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := c.allDone
	if ret {
		log.Println("Exit")
	}
	c.mu.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		currentPhase:      MapPhase,
		sourceFiles:       files,
		mapCnt:            len(files),
		reduceCnt:         nReduce,
		runningMapCnt:     0,
		finishedMapCnt:    0,
		runningReduceCnt:  0,
		finishedReduceCnt: 0,
		allDone:           false,
		mapTaskChan:       make(chan *Task, len(files)),
		mapTaskState:      make([]TaskState, len(files)),
		reduceTaskChan:    make(chan *Task, nReduce),
		reduceTaskState:   make([]TaskState, nReduce),
	}
	c.cond = sync.NewCond(&c.mu)
	// Your code here.

	go c.produceMapTask()
	go c.produceReduceTask()

	c.server()
	return &c
}
