package engine

import (
	"connection"
	"fmt"
	"github.com/nporsche/np-golang-logging"
	"github.com/nporsche/np-golang-pool"
	"net"
	"os"
	"util"
)

var logger = logging.MustGetLogger("engine")

func Run(port int, beanstalkdInstances []string, maxIdleConnections int) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatal("Listen port failure")
		os.Exit(1)
	}

	util.BufferPool = pool.NewBufferPool(10)

	createConnFunc := func(addr string) (ic pool.IConnection, err error) {
		ic, err = connection.NewBackwardConnection(addr)
		return
	}

	util.ConnPool = pool.NewConnPool(
		maxIdleConnections,
		createConnFunc)

	util.IdMgr = util.NewIdManager(beanstalkdInstances)

	sigs := make([]chan bool, 0)
	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Warning("Accept Error")
		}
		ch := make(chan bool, 1)
		sigs = append(sigs, ch)
		fwConn := connection.NewForwardConnection(conn, beanstalkdInstances)
		go fwConn.Handle(ch)
	}
	for _, ch := range sigs {
		<-ch
	}
}
