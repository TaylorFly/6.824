package main

import "fmt"

func main() {
	v := 1
	switch v {
	case 1:
		fmt.Println(1)
		fallthrough
	case 2:
		fmt.Println(2)
	default:
		fmt.Println("none")
	}
}
