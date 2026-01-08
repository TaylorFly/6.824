package main

import (
	"os"
)

func main() {
	err := os.MkdirAll("tmp", 0755)
	if err != nil {
		println(err)
	}
	tf, _ := os.CreateTemp("tmp", "fuck.txt")
	os.Rename(tf.Name(), "a.txt")
	// if err != nil {
	// 	fmt.Print("rename")
	// }
}
