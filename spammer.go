package main

import (
	"fmt"
	"sort"
	"sync"
	// "fmt"
)

func RunPipeline(cmds ...cmd) {
	var wg sync.WaitGroup
	in := make(chan interface{})

	for i, c := range cmds {
		out := make(chan interface{})
		wg.Add(1)
		// email->out->in->
		go func(c cmd, in, out chan interface{}, last bool) {
			defer wg.Done()
			c(in, out)
			if !last {
				close(out)
			}
		}(c, in, out, i == len(cmds)-1)

		in = out
	}

	close(in)
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	// 	in - string
	// 	out - User
	var wg sync.WaitGroup
	mapUsers := make(map[User]struct{})
	mu := new(sync.Mutex)
	for email := range in {
		wg.Add(1)
		email := email.(string)
		go func() {
			defer wg.Done()
			user := GetUser(email)
			mu.Lock()
			defer mu.Unlock()
			if _, exist := mapUsers[user]; !exist {
				mapUsers[user] = struct{}{}
				out <- user
			}
		}()
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	// 	in - User
	// 	out - MsgID
	var wg sync.WaitGroup
	usersBatch := make([]User, 0, GetMessagesMaxUsersBatch)
	goGetMsg := func(users []User) {
		defer wg.Done()
		msgIDs, _ := GetMessages(users...)
		for _, msgID := range msgIDs {
			out <- msgID
		}
	}

	for user := range in {
		user := user.(User)
		usersBatch = append(usersBatch, user)
		if len(usersBatch) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go goGetMsg(usersBatch)
			usersBatch = make([]User, 0, GetMessagesMaxUsersBatch)
		}
	}

	if len(usersBatch) > 0 {
		wg.Add(1)
		go goGetMsg(usersBatch)
	}

	wg.Wait()
}

func startWorker(in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for id := range in {
		id := id.(MsgID)
		result, _ := HasSpam(id)
		out <- MsgData{id, result}
	}
}

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData
	var wg sync.WaitGroup
	
	wg.Add(HasSpamMaxAsyncRequests)
	for i := 0; i < HasSpamMaxAsyncRequests; i++ {
		go startWorker(in, out, &wg)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
	var msgs []MsgData
	for msg := range in {
		msgs = append(msgs, msg.(MsgData))
	}

	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].HasSpam == msgs[j].HasSpam {
			return msgs[i].ID < msgs[j].ID
		}
		return msgs[i].HasSpam && !msgs[j].HasSpam
	})

	for _, msg := range msgs {
		out <- fmt.Sprintf("%t %d", msg.HasSpam, msg.ID)
	}
}
