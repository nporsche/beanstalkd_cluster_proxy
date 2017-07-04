package integration

import "github.com/nutrun/lentil"
import "log"

import "fmt"
import "testing"

func getConn() *lentil.Beanstalkd {
	conn, e := lentil.Dial("localhost:11333")
	if e != nil {
		log.Fatal(e)
	}
	return conn
}

func Test_lentil(t *testing.T) {
	beanstalkd := getConn()
	// cleanup beanstalkd
	tubes, e := beanstalkd.ListTubes()
	if e != nil {
		t.Fatal(e)
	}
	for _, tube := range tubes {
		beanstalkd.Watch(tube)
	}
	for {
		job, e := beanstalkd.ReserveWithTimeout(0)
		if e == nil {
			beanstalkd.Delete(job.Id)
		} else {
			break
		}
	}
	for _, tube := range tubes {
		if tube != "default" {
			beanstalkd.Ignore(tube)
		}
	}
	//end clean beanstalkd

	e = beanstalkd.Use("rock")
	if e != nil {
		t.Error(e)
	}
	id, e := beanstalkd.Put(0, 0, 10, []byte("y u no is job?"))
	if e != nil || id == 0 {
		t.Error(e)
	}
	watching, e := beanstalkd.Watch("rock")
	if e != nil {
		t.Error(e)
	}
	if watching != 2 {
		t.Error("Y U NO WATCHIN 2 TUBS?")
	}
	job, e := beanstalkd.Reserve()
	if e != nil {
		t.Error(e)
	}
	if string(job.Body) != "y u no is job?" {
		t.Error(fmt.Sprintf("[%s] isn't [%s]", job.Body, "y u no is job?"))
	}
	e = beanstalkd.Delete(job.Id)
	if e != nil {
		t.Error(e)
	}
	_, e = beanstalkd.ReserveWithTimeout(0)
	if e == nil {
		t.Error("Y U NO TIME OUT?")
	}

	//no job at all, watching rock and default, use rock
	watching, e = beanstalkd.Ignore("dontexist")
	if e != nil {
		t.Error(e)
	}
	if watching != 2 {
		t.Error("Y U NO WATCH 2 TUBS?")
	}
	watching, e = beanstalkd.Ignore("rock")
	if e != nil {
		t.Error(e)
	}
	if watching != 1 {
		t.Error("Y U NO WATCH 1 TUB?")
	}
	_, e = beanstalkd.Ignore("default")
	if e == nil {
		t.Error("Y U NO ERROR?")
	}
	beanstalkd.Use("default")
	//no job at all, only watching default, use default
	beanstalkd.Put(0, 0, 10, []byte("job 2"))
	job, _ = beanstalkd.Reserve()
	e = beanstalkd.Release(job.Id, 0, 0)
	if e != nil {
		t.Error(e)
	}
	job, _ = beanstalkd.Reserve()
	e = beanstalkd.Touch(job.Id)
	if e != nil {
		t.Error(e)
	}
	e = beanstalkd.Bury(job.Id, 0)
	if e != nil {
		t.Error(e)
	}
	job, e = beanstalkd.PeekBuried()
	if e != nil {
		t.Error(e)
	}
	if string(job.Body) != "job 2" {
		t.Error("Peeked wrong job")
	}
	/*
		does not support "kick" command
		count, e := beanstalkd.Kick(1)
		if e != nil {
			t.Error(e)
		}
		if count != 1 {
			t.Error("Y U NO KIK?")
		}
		job, e = beanstalkd.Peek(job.Id)
		if e != nil {
			t.Error(e)
		}
		if string(job.Body) != "job 2" {
			t.Error("Peeked wrong job")
		}
	*/
	_, e = beanstalkd.StatsJob(job.Id)
	if e != nil {
		t.Error(e)
	}
	_, e = beanstalkd.StatsTube("default")
	if e != nil {
		t.Error(e)
	}
	if e != nil {
		t.Error()
	}
	tubes, e = beanstalkd.ListTubes()
	if e != nil {
		t.Error(e)
	}
	if tubes[0] != "default" {
		t.Error("Y U NO HAVE RITE TUB?")
	}
	tube, e := beanstalkd.ListTubeUsed()
	if e != nil {
		t.Error(e)
	}
	if tube != "default" {
		t.Error("Watching wrong tube")
	}
	tubes, e = beanstalkd.ListTubesWatched()
	if e != nil {
		t.Error(e)
	}
	if len(tubes) != 1 {
		t.Error(len(tubes))
	}
	if tubes[0] != "default" {
		t.Error("Y U NO HAVE RITE TUB?")
	}
	e = beanstalkd.PauseTube("default", 1)
	if e != nil {
		t.Error(e)
	}
	beanstalkd.Quit()
}
