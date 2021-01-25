package main

import (
	"fmt"
	"sync"
	"time"
)

type Task interface {
	Execute(int) (int, error)
}

type adder struct {
	augend int
}

type lazyAdder struct {
	adder
	delay time.Duration
}

type pipeline struct {
	list []Task
}

type fastest struct {
	fast_list []Task
}

type timed struct {
	task    Task
	timeout time.Duration
}

type concurrent struct {
	list   []Task
	reduce func([]int) int
}

type searcher struct {
	errorLimit int
	input      chan Task
}

func (a adder) Execute(addend int) (int, error) {
	result := a.augend + addend
	if result > 127 {
		return 0, fmt.Errorf("Result %d exceeds the adder threshold", a)
	}
	return result, nil
}

func (la lazyAdder) Execute(addend int) (int, error) {
	time.Sleep(la.delay * time.Millisecond)
	return la.adder.Execute(addend)
}

// Execute interface for pipeline struct (from Pipeline(tasks... Task) function)
func (p pipeline) Execute(retVal int) (_ int, retErr error) {
	// If task list is empty return error
	if len(p.list) == 0 {
		return -3, fmt.Errorf("Pipeline task list is empty!")
	}
	// For each task do Execute()
	for i, t := range p.list {
		// If task is nil return error
		if t == nil {
			return -2, fmt.Errorf("Task %d is nil!", i)
		}
		retVal, retErr = t.Execute(retVal)
		// If Execute() returned error -> return error
		if retErr != nil {
			return -1, fmt.Errorf("Task %d failed! %s", i, retErr.Error())
		}
	}
	return retVal, retErr
}

// Execute interface for fastest struct (from Fastest(tasks... Task) function)
func (f fastest) Execute(param int) (int, error) {
	var wg sync.WaitGroup

	// If task list is empty return error
	if len(f.fast_list) == 0 {
		return -3, fmt.Errorf("Fastest task list is empty!")
	}

	// Make a channel for the return values from the goroutines
	// and a channel to signal to stop sending return values
	retChannel := make(chan struct {
		val int
		err error
	})
	defer close(retChannel)
	stopChannel := make(chan struct{})

	// Variable to hold the result
	var result struct {
		val int
		err error
	}

	// "Receiver" goroutine
	wg.Add(1)
	go func(c chan struct {
		val int
		err error
	}) {
		result = <-retChannel
		close(stopChannel)
		wg.Done()
	}(retChannel)

	// Iterate over each task in the list
	for i, t := range f.fast_list {
		// If a task is nil return error
		if t == nil {
			wg.Wait()
			return -2, fmt.Errorf("Task %d is nil!", i)
		}
		// Do Execute() on each task, "Sender" goroutine
		wg.Add(1)
		go func(task Task) {

			retVal, retErr := task.Execute(param)
			stopSend := false
			for {
				if stopSend {
					break
				}
				// If stopChannel is closed -> raise flag
				select {
				case <-stopChannel:
					stopSend = true
				// The fastest Task will send its result and after that the other goroutine will close the stopChannel
				case retChannel <- struct {
					val int
					err error
				}{retVal, retErr}:
				}
			}
			wg.Done()
		}(t)
	}
	wg.Wait()
	if result.err != nil {
		return -1, fmt.Errorf("Task failed! %s", result.err.Error())
	}
	return result.val, result.err
}

// Execute interface for the timed struct (from Timed(task Task, timeout time.Duration) function)
func (t timed) Execute(param int) (int, error) {
	// If task is nil return error
	if t.task == nil {
		return -2, fmt.Errorf("Timed task is nil!\n")
	}

	// Make a channel for the task results
	retChannel := make(chan struct {
		val int
		err error
	})
	defer close(retChannel)

	// Execute task, "Sender" goroutine
	go func(task Task) {
		retVal, retErr := task.Execute(param)
		retChannel <- struct {
			val int
			err error
		}{retVal, retErr}
	}(t.task)

	// Timeout pattern
	select {
	case ret := <-retChannel:
		if ret.err != nil {
			return -1, fmt.Errorf("Task failed! %s", ret.err.Error())
		}
		return ret.val, ret.err
	case <-time.After(t.timeout):
		return -1, fmt.Errorf("Timed task reached timeout!")
	}
}

// Execute interface for the concurrent struct
func (c concurrent) Execute(param int) (int, error) {
	var wg sync.WaitGroup

	// If task list is emtpy -> return error
	if len(c.list) == 0 {
		return -3, fmt.Errorf("Concurrent task list is empty!")
	}

	var results []struct {
		val int
		err error
	}

	// Make a channel for the results
	retChannel := make(chan struct {
		val int
		err error
	})
	defer close(retChannel)

	stopChannel := make(chan struct{})

	// "Receiver" goroutine
	go func() {
		for {
			select {
			case <-stopChannel:
				return
			case result := <-retChannel:
				results = append(results, result)
				wg.Done()
			}
		}
	}()

	for i, t := range c.list {
		// If task is nil -> return error
		if t == nil {
			wg.Wait()
			return -2, fmt.Errorf("Task %d is nil!", i)
		}

		// "Sender" goroutine
		wg.Add(1)
		go func(t Task) {
			retVal, retErr := t.Execute(param)
			retChannel <- struct {
				val int
				err error
			}{retVal, retErr}
		}(t)
	}

	wg.Wait()
	close(stopChannel)

	reduceSlice := []int{}
	for _, result := range results {
		if result.err != nil {
			return -1, fmt.Errorf("Task failed!%s", result.err.Error())
		}
		reduceSlice = append(reduceSlice, result.val)
	}
	fmt.Println(reduceSlice)
	return c.reduce(reduceSlice), nil
}

func (s searcher) Execute(param int) (int, error) {
	var wg sync.WaitGroup

	errorCnt := 0
	// Make a channel for return values
	retChannel := make(chan struct {
		val int
		err error
	})

	// Make a stop channel for signaling to stop receiving
	stopChannel := make(chan struct{})

	// Variable to hold the results from the "Sender" goroutines
	var results []struct {
		val int
		err error
	}

	// "Receiver" goroutine
	go func() {
		defer close(retChannel)
		for {
			select {
			case result := <-retChannel:
				results = append(results, result)
				wg.Done()
			case <-stopChannel:
				return
			}
		}
	}()

	// Iterate through range over the input channel
	// Once it is closed we can calculate all the results
	// If we want to stop the execution early (e.g. when error limit is reached)
	// we can use a "stopChannel" to signal the input channel
	for t := range s.input {
		wg.Add(1)
		go func(param int, task Task) {
			retVal, retErr := task.Execute(param)
			retChannel <- struct {
				val int
				err error
			}{retVal, retErr}
		}(param, t)
	}

	wg.Wait()
	close(stopChannel)

	var maxRes int
	for _, elem := range results {
		if elem.err != nil {
			errorCnt++
			if errorCnt > s.errorLimit {
				return -1, fmt.Errorf("Too many tasks have failed!")
			}
		} else {
			if maxRes < elem.val {
				maxRes = elem.val
			}
		}
	}
	return maxRes, nil
}

// Pipeline : Returns a pipeline object with all the tasks
func Pipeline(tasks ...Task) Task {
	var result pipeline
	result.list = append(result.list, tasks...)

	return result
}

// Fastest : Returns a fastest object with all the tasks
func Fastest(tasks ...Task) Task {
	var result fastest
	result.fast_list = append(result.fast_list, tasks...)
	return result
}

// Timed : Returns a timed object with the task and the timeout
func Timed(task Task, timeout time.Duration) Task {
	return timed{task, timeout}
}

// ConcurrentMapReduce : Returns a concurrent object with all the tasks and the reduce function
func ConcurrentMapReduce(reduce func(results []int) int, tasks ...Task) Task {
	var result concurrent
	result.list = append(result.list, tasks...)
	result.reduce = reduce
	return result
}

// GreatestSearcher : Returns searcher object with a tasks channel and error limit
func GreatestSearcher(errorLimit int, tasks chan Task) Task {
	var result searcher
	result.input = tasks
	result.errorLimit = errorLimit
	return result
}

func main() {
	if res, err := Pipeline(adder{50}, adder{60}).Execute(10); err != nil {
		fmt.Printf("The pipeline returned an error! %s!\n", err.Error())
	} else {
		fmt.Printf("The pipeline returned %d!\n", res)
	}

	if res, err := Fastest(lazyAdder{adder{20}, 500}, lazyAdder{adder{50}, 300}, adder{41}).Execute(1); err != nil {
		fmt.Printf("The fast pipeline returned an error! %s\n", err.Error())
	} else {
		fmt.Printf("The fast pipeline returned %d!\n", res)
	}

	if res, err := Timed(lazyAdder{adder{20}, 50}, 300*time.Millisecond).Execute(2); err != nil {
		fmt.Printf("The timed task returned an error! %s\n", err.Error())
	} else {
		fmt.Printf("The timed task returned %d\n", res)
	}

	reduce := func(results []int) int {
		smallest := 128
		for _, v := range results {
			if v < smallest {
				smallest = v
			}
		}
		return smallest
	}

	mr := ConcurrentMapReduce(reduce, adder{30}, adder{10}, adder{20})
	if res, err := mr.Execute(5); err != nil {
		fmt.Printf("We got an error! %s", err.Error())
	} else {
		fmt.Printf("The ConcurrentMapReduce returned %d\n", res)
	}

	tasks := make(chan Task)
	gs := GreatestSearcher(2, tasks) // Приемаме 2 грешки

	go func() {
		tasks <- adder{4}
		tasks <- lazyAdder{adder{22}, 20}
		tasks <- adder{125} // Това е първата "допустима" грешка (защото 125+10 > 127)
		time.Sleep(50 * time.Millisecond)
		tasks <- adder{32} // Това би трябвало да "спечели"

		// Това би трябвало да timeout-не и да е втората "допустима" грешка
		tasks <- Timed(lazyAdder{adder{100}, 2000}, 20*time.Millisecond)

		// Ако разкоментираме това, gs.Execute() трябва да върне грешка
		// tasks <- adder{127} // трета (и недопустима) грешка
		close(tasks)
	}()
	result, err := gs.Execute(10)
	fmt.Println(result, err)
}
