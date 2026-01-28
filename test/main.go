package main

import "fmt"

func main() {
	a := make([]int, 10)
	for i := range 10 {
		a[i] = i
	}
	fmt.Println(a[:2])
}
