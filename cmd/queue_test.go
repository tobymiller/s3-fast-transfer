package cmd

import (
	"reflect"
	"testing"
)

func F(i interface{}, plus interface{}) (interface{}, error) {
	return i.(int) + plus.(int), nil
}
func T() (interface{}, error) {
	return 2, nil
}
func TestRunThreads(t *testing.T) {
	result := RunThreads(F, []interface{} {1, 2, 3}, T, 2)
	if !reflect.DeepEqual(result, []interface{} {3, 4, 5}) {
		t.Errorf("Failure in threads test")
	}
}