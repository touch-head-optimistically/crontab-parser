package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func addCron(j int) {

	filpath := fmt.Sprintf("/etc/cron.d/crontest_%d", j)
	file, _ := os.OpenFile(filpath, os.O_CREATE|os.O_RDWR, 0666)

	defer file.Close()

	cmdInfo := fmt.Sprintf("* * * * * root date > /root/crontest/%d.txt", j)
	file.WriteString(cmdInfo + "\n")
}

func main() {

	fmt.Println("This is test for testing cron I/O")

	if len(os.Args) == 1 {
		os.Args[1] = "1000"
	}

	time.Sleep(1)

	i := 0
	num, _ := strconv.Atoi(os.Args[1])

	fmt.Println("now add 1000 cron files...")

	for i < num {
		go addCron(i)
		i = i + 1
	}

	fmt.Println("all success!")

	return
}
