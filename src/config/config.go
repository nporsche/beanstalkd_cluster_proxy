package config

import (
	"flag"
	goyaml "github.com/nporsche/goyaml"
	"io/ioutil"
	"log"
)

type info struct {
	Beanstalkd         []string
	ListenPort         int
	Processor          int
	MaxIdleConnections int
}

var BstdRouter info

func Reload(path string) {
	var content []byte
	var err error
	if content, err = ioutil.ReadFile(path); err != nil {
		log.Fatal("ReloadConfig failure from path:" + path)
	}
	if err := goyaml.Unmarshal(content, &BstdRouter); err != nil {
		log.Fatal("ReloadConfig unmarshal failure from path:" + path)
	}
	return
}

func init() {
	config_path := flag.String("C", "conf/config.yaml", "The path of bstdrouter config file")
	flag.Parse()
	Reload(*config_path)
}
