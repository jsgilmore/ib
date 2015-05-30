package ib

import (
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Test if random signals cause infiniband errors. Poll did not check for EINTR and could be interrupted causing errors
// to be returned. Needs -cpu=2 or higher to trigger the issue.
func TestConnWithSignals(t *testing.T) {
	for i := 0; i < 10; i++ {
		testConnWithSignals(t)
	}
}

func testConnWithSignals(t *testing.T) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go signalSelf(stop, &wg)
	connectReadAndWrite(t)
	close(stop)
	wg.Wait()
}

func signalSelf(stop chan struct{}, wg *sync.WaitGroup) {
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
		case <-stop:
			wg.Done()
			return
		}
	}
}

func connectReadAndWrite(t *testing.T) {
	laddr, raddr := chooseInterfaces(t)
	sendMr, err := AllocateMemory(benchMrLen)
	if err != nil {
		panic(err)
	}
	populateRegion(sendMr)

	recvMr, err := AllocateMemory(sendMr.Len())
	if err != nil {
		panic(err)
	}

	l, err := ListenRC(raddr)
	if err != nil {
		panic(err)
	}
	raddr = l.Addr().(*net.TCPAddr)

	ch := make(chan *RCConn)
	go func() {
		c, err := DialRC(laddr, raddr)
		if err != nil {
			panic(err)
		}
		ch <- c
	}()
	srvc, err := l.Accept()
	if err != nil {
		panic(err)
	}
	clic := <-ch

	var wg sync.WaitGroup
	wg.Add(1)
	go benchReader(&wg, srvc, recvMr, 1)
	wg.Add(1)
	go benchWriter(&wg, clic, sendMr, 1)
	wg.Wait()

	if err := l.Close(); err != nil {
		panic(err)
	}
	if err := recvMr.Close(); err != nil {
		panic(err)
	}
	if err := sendMr.Close(); err != nil {
		panic(err)
	}
}
