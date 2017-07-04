package connection

import "net"
import "fmt"
import "util"
import "bufio"

type BackwardConnection struct {
	addr         string
	tubeUsed     string
	tubesWatched []string

	conn  net.Conn
	ioBuf *bufio.ReadWriter
}

//IConnection
func (this *BackwardConnection) HostAddr() string {
	return this.addr
}

func (this *BackwardConnection) Close() {
	this.conn.Close()
}

func NewBackwardConnection(addr string) (*BackwardConnection, error) {
	this := new(BackwardConnection)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	this.conn = conn
	this.addr = addr
	this.tubeUsed = "default"
	this.tubesWatched = []string{"default"}

	this.ioBuf = bufio.NewReadWriter(bufio.NewReaderSize(conn, LINE_BUF_SIZE), bufio.NewWriter(conn))
	return this, nil
}

func (this *BackwardConnection) talk(cmd []byte) (echo []byte, err error) {
	err = this.writeLine(cmd)
	if err != nil {
		return nil, err
	}
	echo, err = this.readLine()
	return
}

func (this *BackwardConnection) watch(tubes []string) error {
	//watch new tube
	for _, tube := range tubes {
		if !util.IsExisted(this.tubesWatched, tube) {
			_, err := this.talk([]byte(fmt.Sprintf(CMD_WATCH_FORMAT, tube)))
			if err != nil {
				return err
			}
		}
	}

	//ignore old tube
	for _, tube := range this.tubesWatched {
		if !util.IsExisted(tubes, tube) {
			_, err := this.talk([]byte(fmt.Sprintf(CMD_IGNORE_FORMAT, tube)))
			if err != nil {
				return err
			}
		}
	}

	//copy
	this.tubesWatched = make([]string, len(tubes))
	copy(this.tubesWatched, tubes)

	return nil
}

func (this *BackwardConnection) use(tube string) error {
	if tube == this.tubeUsed {
		return nil
	}

	_, err := this.talk([]byte(fmt.Sprintf(CMD_USE_FORMAT, tube)))
	if err == nil {
		this.tubeUsed = tube
	}
	return err
}

func (this *BackwardConnection) cleanState(watchedTubes []string) error {
	//watch default first
	_, err := this.talk([]byte("watch default"))
	if err != nil {
		return err
	}

	//ignore all the tubes except default
	if watchedTubes != nil {
		for _, t := range watchedTubes {
			if t == "default" {
				continue
			}
			_, err := this.talk([]byte(CMD_IGNORE + t))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (this *BackwardConnection) writeLine(bs []byte) (err error) {
	err = this.write(bs, false)
	if err != nil {
		return
	}
	err = this.write([]byte("\r\n"), true)
	if err != nil {
		return
	}
	logger.Debug("%s post=[%s]", this.addr, string(bs))
	return
}

func (this *BackwardConnection) write(bs []byte, flush bool) (err error) {
	_, err = this.ioBuf.Write(bs)
	if flush {
		err = this.ioBuf.Flush()
	}
	if err != nil {
		util.ConnPool.TurnOff(this, err)
	}
	return
}

func (this *BackwardConnection) readLine() (line []byte, err error) {
	line, _, err = this.ioBuf.ReadLine()
	if err == nil {
		logger.Debug("%s recv=[%s]", this.addr, string(line))
	} else {
		util.ConnPool.TurnOff(this, err)
	}
	return
}

func (this *BackwardConnection) read(bs []byte) (err error) {
	total := 0
	for {
		var n int
		n, err = this.ioBuf.Read(bs[total:])
		if err != nil {
			break
		}
		total += n
		if total == len(bs) {
			break
		}
	}
	if err != nil {
		util.ConnPool.TurnOff(this, err)
	}
	return
}
