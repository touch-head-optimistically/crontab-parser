package main

import (
	"fmt"
	"math/rand"
	"os/exec"
	"time"

	"github.com/robfig/cron"
)

func main() {
	c := cron.New()
	i := 0

	for i < 1000 {
		var cronCmd = func() {
			cmd := fmt.Sprintf("date > /root/crontest/%d.txt", rand.Intn(10000))
			exec.Command("/bin/sh", "-c", cmd).Output()
		}

		c.AddFunc("0 */1 * * * *", cronCmd)
		i = i + 1
	}
	c.Start()

	fmt.Printf("all cronTask is running....now is: %s", time.Now().Format("2006-01-02 15:04:05"))
	select {}
}
