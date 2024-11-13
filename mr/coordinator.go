package mr

import (
	"log"
	pb "mapreduce/proto"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Coordinator struct {
	pb.UnimplementedMapReduceServer
	taskList      chan *pb.Task
	taskIndex     map[int64]*pb.Task
	status        int
	reduceNum     int
	files         []string
	intermediates [][]string
	outFile       []string
	lock          *sync.RWMutex
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		taskList:      make(chan *pb.Task, max(len(files), nReduce)),
		taskIndex:     make(map[int64]*pb.Task),
		status:        Map,
		reduceNum:     nReduce,
		files:         files,
		intermediates: make([][]string, nReduce),
		outFile:       make([]string, nReduce),
		lock:          new(sync.RWMutex),
	}
	c.CreateMapTask()
	c.server()
	go c.CheckCrash()
	return &c
}

// CheckCrash 检测crash
func (c *Coordinator) CheckCrash() {
	for true {
		c.lock.Lock()
		if c.status == Exit {
			c.lock.Unlock()
			break
		}
		for i := range c.taskIndex {
			if ((c.taskIndex[i].TaskStage == Map) || (c.taskIndex[i].TaskStage == Reduce)) && (time.Unix(c.taskIndex[i].StartTime.GetSeconds(), 0) != time.Time{}) && (time.Since(time.Unix(c.taskIndex[i].StartTime.GetSeconds(), 0)).Seconds() >= 10) {
				c.taskIndex[i].StartTime = timestamppb.New(time.Time{})
				if c.status == Map {
					c.taskIndex[i].TaskStage = Map
				} else if c.status == Reduce {
					c.taskIndex[i].TaskStage = Reduce
				}
				c.taskList <- c.taskIndex[i]
			}
		}
		c.lock.Unlock()
		time.Sleep(time.Second * 2)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(ctx context.Context, args *pb.GetTaskArgs) (*pb.GetTaskReply, error) {
	c.lock.Lock()
	reply := &pb.GetTaskReply{}
	if len(c.taskList) > 0 {
		reply.Task = <-c.taskList
		c.taskIndex[reply.Task.TaskId].StartTime = timestamppb.New(time.Now())
	} else {
		if c.status == Exit {
			reply.Task = &pb.Task{
				TaskStage: Exit,
				TaskId:    -1,
				ReduceNum: -1,
				FileName:  "",
			}
		} else {
			reply.Task = &pb.Task{
				TaskStage: Wait,
				TaskId:    -1,
				ReduceNum: -1,
				FileName:  "",
			}
		}
	}
	c.lock.Unlock()
	return reply, nil
}

func (c *Coordinator) CompleteTask(ctx context.Context, args *pb.CompleteTaskArgs) (*pb.CompleteTaskReply, error) {
	id, stage, files := args.TaskId, int(args.Stage), args.FilePaths
	reply := &pb.CompleteTaskReply{}
	c.lock.Lock()
	if stage != c.status || c.taskIndex[id].TaskStage == MapComplete || c.taskIndex[id].TaskStage == ReduceComplete {
		c.lock.Unlock()
		return reply, nil
	}
	if stage == Map {
		c.taskIndex[id].TaskStage = MapComplete
		for i, v := range files {
			c.intermediates[i] = append(c.intermediates[i], v)
		}
	} else if stage == Reduce {
		c.taskIndex[id].TaskStage = ReduceComplete
		c.outFile[id] = files[0]
	}
	c.lock.Unlock()
	go c.CheckStage(stage)
	return reply, nil
}

func (c *Coordinator) CheckStage(stage int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if stage == Map && c.isAllComplete() {
		c.status = Reduce
		c.CreateReduceTask()
	} else if stage == Reduce && c.isAllComplete() {
		c.status = Exit
	}
}

func (c *Coordinator) isAllComplete() bool {
	for i := range c.taskIndex {
		if c.taskIndex[i].TaskStage == Map || c.taskIndex[i].TaskStage == Reduce {
			return false
		}
	}
	return true
}

// use files to create tasks
func (c *Coordinator) CreateMapTask() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i, f := range c.files {
		task := pb.Task{
			TaskStage:     Map,
			TaskId:        int64(i),
			ReduceNum:     int64(c.reduceNum),
			Intermediates: nil,
			FileName:      f,
			StartTime:     timestamppb.New(time.Time{}),
		}
		c.taskList <- &task
		c.taskIndex[int64(i)] = &task
	}
}

func (c *Coordinator) CreateReduceTask() {
	c.taskIndex = make(map[int64]*pb.Task)
	for i, files := range c.intermediates {
		task := pb.Task{
			TaskStage:     Reduce,
			TaskId:        int64(i),
			ReduceNum:     int64(c.reduceNum),
			Intermediates: files,
			FileName:      "",
			StartTime:     timestamppb.New(time.Time{}),
		}
		c.taskList <- &task
		c.taskIndex[int64(i)] = &task
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	server := grpc.NewServer()
	pb.RegisterMapReduceServer(server, c)

	// rpc.Register(c)
	// rpc.HandleHTTP()
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	// go http.Serve(l, nil)
	server.Serve(l)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.status == Exit {
		time.Sleep(time.Second * 5)
		return true
	}
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
