package connection

import "net"
import "github.com/nporsche/np-golang-logging"
import "github.com/nporsche/np-golang-pool"
import "util"
import "runtime/debug"
import "bufio"
import "time"
import "math/rand"
import "io"

var logger = logging.MustGetLogger("connection")

type ForwardConnection struct {
	conn  net.Conn
	ioBuf *bufio.ReadWriter

	// order map
	hostKey []string
	connVal []*BackwardConnection

	tubeUsed     string
	tubesWatched []string
}

func NewForwardConnection(conn net.Conn, bwConns []string) *ForwardConnection {
	this := new(ForwardConnection)
	this.conn = conn
	this.ioBuf = bufio.NewReadWriter(bufio.NewReaderSize(conn, LINE_BUF_SIZE), bufio.NewWriter(conn))

	this.tubeUsed = "default"
	this.tubesWatched = []string{"default"}

	//init bw connections
	hostLen := len(bwConns)
	this.hostKey = make([]string, hostLen)
	this.connVal = make([]*BackwardConnection, hostLen)

	rand.Seed(time.Now().UnixNano())
	randSeq := rand.Perm(hostLen)
	for i, seq := range randSeq {
		this.hostKey[i] = bwConns[seq]
	}
	//end retrieve set of connections

	return this
}

func (this *ForwardConnection) Handle(sign chan bool) {
	defer func() {
		if x := recover(); x != nil && x != CMD_QUIT {
			logger.Error("%v STACK TRACE:%s", x, string(debug.Stack()))
		}
		logger.Info("%s is disconnected", this.conn.RemoteAddr().String())

		for _, bwConn := range this.connVal {
			if bwConn != nil {
				bwConn.cleanState(this.tubesWatched)
				util.ConnPool.Return(bwConn)
			}
		}
		this.conn.Close()
		sign <- true
	}()

	for {
		line, err := this.readLine()
		if err != nil {
			break
		}
		logger.Debug("Handle command=[%s]", string(line)) //cmd does not include "\r\n"
		this.HandleCmd(line)
	}
}

func (this *ForwardConnection) writeLine(bs []byte) (err error) {
	err = this.write(bs, false)
	if err != nil {
		return
	}
	err = this.write([]byte("\r\n"), true)
	if err != nil {
		return
	}
	return
}

func (this *ForwardConnection) write(bs []byte, flush bool) (err error) {
	_, err = this.ioBuf.Write(bs)
	if flush {
		err = this.ioBuf.Flush()
	}
	return
}

func (this *ForwardConnection) readLine() (line []byte, err error) {
	line, _, err = this.ioBuf.ReadLine()
	return
}

func (this *ForwardConnection) read(bs []byte) (err error) {
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
	return
}

func (this *ForwardConnection) closed() bool {
	one := make([]byte, 1, 1)
	this.conn.SetReadDeadline(time.Now().Add(time.Second))
	var err error
	closed := false
	if _, err = this.conn.Read(one); err == io.EOF {
		closed = true
	}
	return closed
}

func (this *ForwardConnection) talkToBackward(cmd []byte, bwConnFilter skipFunc, handler responseHandler) {
	for i, host := range this.hostKey {
		if util.ConnPool.Health(host).State == pool.StateDisconnected {
			this.connVal[i] = nil
			continue
		}
		if this.connVal[i] == nil {
			conn, err := util.ConnPool.GetByHost(host)
			if err != nil {
				continue
			}
			this.connVal[i] = conn.(*BackwardConnection)
		}

		conn := this.connVal[i]
		//filter
		if bwConnFilter(conn) {
			continue
		}

		//lazy use
		err := conn.use(this.tubeUsed)
		if err != nil {
			conn.Close()
			this.connVal[i] = nil
			continue
		}

		//lazy watch
		err = conn.watch(this.tubesWatched)
		if err != nil {
			conn.Close()
			this.connVal[i] = nil
			continue
		}

		err = conn.writeLine(cmd)
		if err != nil {
			conn.Close()
			this.connVal[i] = nil
			continue
		}

		if handler(conn) {
			break
		}
	}
	return
}
