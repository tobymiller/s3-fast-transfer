// I originally wrote this all generically until I realised go doesn't actually have generics yet.
// Left it here as it's testable and it can be put back when generics are introduced into the language.

package cmd

type ThreadDataFunc func() (interface{}, error, func() error)
type ThreadFunc func(input interface{}, threadData interface{}) (interface{}, error)

func RunThreads(threadFunc ThreadFunc, inputs []interface{}, threadDataFunc ThreadDataFunc, threadCount int) []interface{} {
	type Indexed struct {
		i int
		t interface{}
	}

	type ThreadResponse struct {
		index int
		response interface{}
		err error
	}

	tasks := make(chan Indexed, threadCount)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan ThreadResponse, len(inputs))

	// create the workers for all the threads in this test
	for w := 1; w <= threadCount; w++ {
		go func(tasks <-chan Indexed, results chan<- ThreadResponse) {
			threadData, err, dispose := threadDataFunc()
			if err != nil {
				panic(err)
			}
			for task := range tasks {
				r, err := threadFunc(task.t, threadData)
				results <- ThreadResponse{
					index:  task.i,
					response: r,
					err:      err,
				}
			}
			err = dispose()
			if err != nil {
				panic(err)
			}
		}(tasks, results)
	}

	for i := 0; i < len(inputs); i++ {
		tasks <- Indexed{
			i: i,
			t: inputs[i],
		}
	}

	close(tasks)
	resultSlice := make([]interface{}, len(inputs))

	for i := 0; i < len(inputs); i++ {
		result := <- results
		if result.err != nil {
			panic(result.err) // for now
		}
		resultSlice[result.index] = result.response
	}
	return resultSlice
}