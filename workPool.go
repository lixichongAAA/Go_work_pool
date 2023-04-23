package main

import (
	"fmt"
	"reflect"
	"time"
)

// Task 无实质性内容，这里仅定义一个整形变量
type Task struct {
	Num int
}

type Job struct {
	Task Task
}

// 这两个全局变量用于模拟
var (
	MaxWorker = 5
	JobQueue  chan Job // 工作通道，模拟需要处理的工作
)

type Worker struct {
	id         int           // id
	WorkerPool chan chan Job // 工作者池，每个元素是一个job通道，所有worker共享一个WorkerPool
	JobChannel chan Job      // 工作通道，属于当前Worker的私有工作队列
	exit       chan bool     // 结束信号
}

// 创建一个新的 worker
func NewWorker(workerPool chan chan Job, id int) Worker {
	fmt.Printf("New a worker (%d)\n", id)
	return Worker{
		id:         id,
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		exit:       make(chan bool),
	}
}

// 监听任务和结束信号
func (w Worker) Start() {
	go func() {
		for {
			// 将当前任务队列注册到工作池
			w.WorkerPool <- w.JobChannel
			fmt.Println("Register private JobChannel to public WorkPool ", w)
			select {
			case job := <-w.JobChannel: // 收到任务
				fmt.Println("Get a job from private w.JobChannel")
				fmt.Println(job)
				time.Sleep(1 * time.Second)
			case <-w.exit: // 收到结束信号
				fmt.Println("Worker exit: ", w)
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.exit <- true
	}()
}

// 排程中心
type Scheduler struct {
	WorkerPool chan chan Job // 工作池
	MaxWorkers int           // 最大工作者数
	Workers    []*Worker     // worker 队列
}

// 创建排程中心
func NewScheduler(maxWorkers int) *Scheduler {
	pool := make(chan chan Job, maxWorkers)
	return &Scheduler{WorkerPool: pool, MaxWorkers: maxWorkers}
}

// 工作池的初始化
func (s *Scheduler) Create() {
	workers := make([]*Worker, s.MaxWorkers)
	for i := 0; i < s.MaxWorkers; i++ {
		worker := NewWorker(s.WorkerPool, i)
		worker.Start()
		workers[i] = &worker
	}
	s.Workers = workers
	go s.schedule()
}

// 工作池的关闭
func (s *Scheduler) Shutdown() {
	workers := s.Workers
	for _, w := range workers {
		w.Stop()
	}
	time.Sleep(time.Second)
	close(s.WorkerPool)
}

// 排程
func (s *Scheduler) schedule() {
	for {
		select {
		case job := <-JobQueue:
			fmt.Println("Get a job from JobQueue")
			go func(job Job) {
				// 从 WorkePool 获取 JobChannel, 忙时阻塞
				jobChannel := <-s.WorkerPool
				fmt.Println("Get a private jobChannel from public s.WorkerPool", reflect.TypeOf(jobChannel))
				jobChannel <- job
				fmt.Println("Worker's private jobChannel add one job")
			}(job)
		}
	}
}

func main() {
	JobQueue = make(chan Job, 5)
	scheduler := NewScheduler(MaxWorker)
	scheduler.Create()
	time.Sleep(1 * time.Second)
	go createJobQueue()
	time.Sleep(30 * time.Second)
	scheduler.Shutdown()
	time.Sleep(10 * time.Second)
}

// 创建模拟任务
func createJobQueue() {
	for i := 0; i < 20; i++ {
		task := Task{Num: 1}
		job := Job{Task: task}
		JobQueue <- job
		fmt.Println("JobQueue add one job")
		time.Sleep(1 * time.Second)
	}
}
