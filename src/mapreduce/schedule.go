package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	tasksDone := 0
	tasksToDo := make(chan int, ntasks)
	for i := 0; i<ntasks;i++ {
		tasksToDo <- i
	}
	loop:
	for {
		var task int
		//get a task or wait for all tasks done
		select {
			case task = <- tasksToDo:
			default:
				if(tasksDone == ntasks) {
					break loop
				}
				continue loop
		}
		//there is a task to do
		wk := <- mr.registerChannel
		var file string
		if(phase == mapPhase) {
			file = mr.files[task]
		}
		args := DoTaskArgs{mr.jobName,file,phase,task,nios}
		go func(args DoTaskArgs, wk string) {
			ok := call(wk, "Worker.DoTask", args, new(struct{}))
			if ok {
				tasksDone++
				mr.registerChannel <- wk
			} else {
				tasksToDo <- task
			}
		}(args, wk)
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
