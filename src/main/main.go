package main

import (
	"config"
	"engine"
	"fmt"
	"github.com/nporsche/np-golang-logging"
	"log/syslog"
	"os"
	"runtime"
)

//import "net/http"
//import _ "net/http/pprof"
var logger = logging.MustGetLogger("main")

func main() {
	/*
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
	*/
	backend, err := logging.NewSyslogBackendPriority("TalkTeller", syslog.LOG_LOCAL3)
	if err != nil {
		fmt.Printf("logging init error=[%s]", err.Error())
		os.Exit(1)
	}
	format := logging.MustStringFormatter("%{color}[%{module}.%{shortfunc}][%{level:.4s}]%{color:reset}%{message}")
	logging.SetBackend(logging.NewBackendFormatter(backend, format))

	runtime.GOMAXPROCS(config.BstdRouter.Processor)
	engine.Run(config.BstdRouter.ListenPort, config.BstdRouter.Beanstalkd, config.BstdRouter.MaxIdleConnections)
}
