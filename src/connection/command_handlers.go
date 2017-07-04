package connection

import "strings"
import "util"
import "errors"
import "fmt"
import "time"

//response handle functor
type responseHandler func(*BackwardConnection) (done bool)
type skipFunc func(*BackwardConnection) (skip bool) //take means, use this conn, otherwise skip

var noSkipFilter = func(*BackwardConnection) bool {
	return false
}

func hostFilter(host string) skipFunc {
	return func(conn *BackwardConnection) bool {
		return conn.addr != host
	}
}

func simpleResponseHandler(response *[]byte, broadcast bool) responseHandler {
	return func(conn *BackwardConnection) bool {
		line, err := conn.readLine()
		if err == nil {
			*response = line
		} else {
			logger.Warning("command=%s response error=%s", line, err.Error())
		}
		return !broadcast
	}
}

func listInfoResponseHandler(response *[]byte) responseHandler {
	return func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err != nil {
			logger.Warning("listInfoResponseHandler readline error=", err.Error())
			return false
		}
		//NOT_FOUND
		if string(line) == REPLY_NOT_FOUND {
			*response = line
			return true
		}

		//OK <bytes>
		var bodyLen int
		n, err := fmt.Sscanf(string(line), "OK %d", &bodyLen)
		if err != nil || n != 1 {
			logger.Warning("listInfoResponseHandler scanf error=", err.Error())
			return false
		}
		wholeCmd := util.BufferPool.Get()
		defer func() {
			util.BufferPool.Release(wholeCmd)
		}()

		wholeCmd.Write(line)
		wholeCmd.WriteString("\r\n")

		body := make([]byte, bodyLen+2)
		err = bwConn.read(body)
		if err != nil || n != 1 {
			logger.Warning("listInfoResponseHandler read body error=", err.Error())
			return false
		}
		wholeCmd.Write(body[:len(body)-2])
		*response = wholeCmd.Bytes()
		return true
	}
}

//end response handler functor

//command handlers begins
//notes the error means very critical issue and it will quit the loop
type cmdHandler func(this *ForwardConnection, cmd []byte) error

func (this *ForwardConnection) HandleCmd(cmd []byte) {
	handler := findHandler(cmd)
	err := handler(this, cmd)

	if err != nil {
		panic(err.Error())
	}
	return
}

/*
post:
put <pri> <delay> <ttr> <bytes>\r\n <data>\r\n
- <pri> is an integer < 2**32. Jobs with smaller priority values will be scheduled before jobs with larger priorities. The most urgent priority is 0; the least urgent priority is 4,294,967,295.
- <delay> is an integer number of seconds to wait before putting the job in the ready queue. The job will be in the "delayed" state during this time.
- <ttr> -- time to run -- is an integer number of seconds to allow a worker to run this job. This time is counted from the moment a worker reserves this job. If the worker does not delete, release, or bury the job within <ttr> seconds, the job will time out and the server will release the job. The minimum ttr is 1. If the client sends 0, the server will silently increase the ttr to 1.
- <bytes> is an integer indicating the size of the job body, not including the trailing "\r\n". This value must be less than max-job-size (default: 2**16).
- <data> is the job body -- a sequence of bytes of length <bytes> from the previous line.

After sending the command line and body, the client waits for a reply, which may be:
- "INSERTED <id>\r\n" to indicate success.
- "BURIED <id>\r\n" if the server ran out of memory trying to grow the priority queue data structure. - <id> is the integer id of the new job
- "EXPECTED_CRLF\r\n" The job body must be followed by a CR-LF pair, that is, "\r\n". These two bytes are not counted in the job size given by the client in the put command line.
- "JOB_TOO_BIG\r\n" The client has requested to put a job with a body larger than max-job-size bytes.
- "DRAINING\r\n" This means that the server has been put into "drain mode" and is no longer accepting new jobs. The client should try another server or disconnect and try again later.
*/
func putHandler(this *ForwardConnection, cmd []byte) error {
	var pri, delay, ttr, bodyLen int
	n, err := fmt.Sscanf(string(cmd), CMD_PUT_FORMAT, &pri, &delay, &ttr, &bodyLen)
	if err != nil || n != 4 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}

	wholeCmd := util.BufferPool.Get()
	defer func() {
		util.BufferPool.Release(wholeCmd)
	}()

	wholeCmd.Write(cmd)
	wholeCmd.WriteString("\r\n")

	body := make([]byte, bodyLen+2) //include \r\n
	err = this.read(body)
	if err != nil {
		return err
	}

	wholeCmd.Write(body[:len(body)-2])

	var response []byte
	handler := func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err == nil {
			response = line
			var hid uint64
			n, err := fmt.Sscanf(string(response), REPLY_INSERTED_FORMAT, &hid)
			if err == nil && n == 1 {
				response = []byte(fmt.Sprintf(REPLY_INSERTED_FORMAT, util.IdMgr.ToGlobalID(bwConn.HostAddr(), hid)))
			}

			return true
		}
		return false
	}
	this.talkToBackward(wholeCmd.Bytes(), noSkipFilter, handler)
	if response != nil {
		return this.writeLine(response)
	} else {
		return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
	}
}

/*
post:
delete <id>\r\n
- <id> is the job id to delete.

response:
- "DELETED\r\n" to indicate success.
- "NOT_FOUND\r\n" if the job does not exist or is not either reserved by the client, ready, or buried. This could happen if the job timed out before the client sent the delete command.
*/
func deleteHandler(this *ForwardConnection, cmd []byte) error {
	var gid uint64
	n, err := fmt.Sscanf(string(cmd), CMD_DELETE_FORMAT, &gid)
	if err != nil || n != 1 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}
	host, ok := util.IdMgr.ToHost(gid)
	if !ok {
		return this.writeLine([]byte(REPLY_NOT_FOUND))
	}

	var response []byte
	this.talkToBackward([]byte(fmt.Sprintf(CMD_DELETE_FORMAT, util.IdMgr.ToHostID(gid))), hostFilter(host), simpleResponseHandler(&response, true))
	if response != nil {
		return this.writeLine(response)
	}
	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
reserve\r\n
Alternatively, you can specify a timeout as follows:
reserve-with-timeout <seconds>\r\n

response:
DEADLINE_SOON\r\n This gives the client a chance to delete or release its reserved job before the server automatically releases it.
TIMED_OUT\r\n If a non-negative timeout was specified and the timeout exceeded before a job became available, or if the client's connection is half-closed, the server will respond with TIMED_OUT. Otherwise, the only other response to this command is a successful reservation in the form of a text line followed by the job body:
RESERVED <id> <bytes>\r\n<data>\r\n
*/
func reserveHandler(this *ForwardConnection, cmd []byte) error {
	bwCmd := []byte(CMD_RESERVE_TIMEOUT + "0")
	var response []byte
	handler := func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err != nil {
			logger.Warning("reserve got response error=", err.Error())
			return false
		}
		if strings.HasPrefix(string(line), REPLY_TIMED_OUT) {
			return false
		}
		if strings.HasPrefix(string(line), REPLY_DEADLINE_SOON) {
			response = line
			return true
		}
		if strings.HasPrefix(string(line), REPLY_RESERVED) {
			var hid uint64
			var bodyLen int
			fmt.Sscanf(string(line), REPLY_RESERVED_FORMAT, &hid, &bodyLen)

			wholeresponse := util.BufferPool.Get()
			defer func() {
				util.BufferPool.Release(wholeresponse)
			}()

			wholeresponse.WriteString(fmt.Sprintf(REPLY_RESERVED_FORMAT, util.IdMgr.ToGlobalID(bwConn.HostAddr(), hid), bodyLen))
			wholeresponse.WriteString("\r\n")

			body := make([]byte, bodyLen+2)
			err = bwConn.read(body)

			wholeresponse.Write(body[:len(body)-2]) //remove the tail \r\n
			response = wholeresponse.Bytes()
			return true
		}
		response = line
		return true
	}
	for {
		this.talkToBackward(bwCmd, noSkipFilter, handler)
		if response != nil {
			return this.writeLine(response)
		} else {
			time.Sleep(5 * time.Second)
		}

		if this.closed() {
			return errors.New(CMD_QUIT)
		}
	}
}

/*
post:
use <tube>\r\n
- <tube> is a name at most 200 bytes. It specifies the tube to use. If the tube does not exist, it will be created.

reply:
USING <tube>\r\n
- <tube> is the name of the tube now being used.
*/

func useHandler(this *ForwardConnection, cmd []byte) (err error) {
	//lazy use, use command will send only before put, peek
	var tube string
	n, err := fmt.Sscanf(string(cmd), CMD_USE_FORMAT, &tube)
	if err != nil || n != 1 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}

	this.tubeUsed = string(tube)

	return this.writeLine([]byte(fmt.Sprintf(REPLY_USING_FORMAT, tube)))
}

/*
post:
release <id> <pri> <delay>\r\n
- <id> is the job id to release.
- <pri> is a new priority to assign to the job.
- <delay> is an integer number of seconds to wait before putting the job in the ready queue. The job will be in the "delayed" state during this time.

reply:
- "RELEASED\r\n" to indicate success.
- "BURIED\r\n" if the server ran out of memory trying to grow the priority queue data structure.
- "NOT_FOUND\r\n" if the job does not exist or is not reserved by the client.
*/
func releaseHandler(this *ForwardConnection, cmd []byte) error {
	var gid uint64
	var pri, delay int
	n, err := fmt.Sscanf(string(cmd), CMD_RELEASE_FORMAT, &gid, &pri, &delay)
	if err != nil || n != 3 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}

	host, ok := util.IdMgr.ToHost(gid)
	if !ok {
		return this.writeLine([]byte(REPLY_NOT_FOUND))
	}

	var response []byte
	this.talkToBackward([]byte(fmt.Sprintf(CMD_RELEASE_FORMAT, util.IdMgr.ToHostID(gid), pri, delay)), hostFilter(host), simpleResponseHandler(&response, false))
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

//lazy watch
func watchHandler(this *ForwardConnection, cmd []byte) error {
	tube := string(cmd)[len(CMD_WATCH):]

	if !util.IsExisted(this.tubesWatched, tube) {
		util.AddToSlice(&this.tubesWatched, tube)
	}

	return this.writeLine([]byte(fmt.Sprintf(REPLY_WATCHING_FORMAT, len(this.tubesWatched))))
}

//lazy ignore
func ignoreHandler(this *ForwardConnection, cmd []byte) error {
	tube := string(cmd)[len(CMD_IGNORE):]

	if len(this.tubesWatched) == 1 {
		return this.writeLine([]byte(REPLY_NOT_IGNORED))
	}
	if util.IsExisted(this.tubesWatched, tube) {
		util.RemoveFromSlice(&this.tubesWatched, tube)
	}

	return this.writeLine([]byte(fmt.Sprintf(REPLY_WATCHING_FORMAT, len(this.tubesWatched))))
}

/*
touch <id>\r\n
- <id> is the ID of a job reserved by the current connection.

There are two possible responses:
- "TOUCHED\r\n" to indicate success.
- "NOT_FOUND\r\n" if the job does not exist or is not reserved by the client.
*/
func touchHandler(this *ForwardConnection, cmd []byte) error {
	var gid uint64
	n, err := fmt.Sscanf(string(cmd), CMD_TOUCH_FORMAT, &gid)
	if err != nil || n != 1 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}
	host, ok := util.IdMgr.ToHost(gid)
	if !ok {
		return this.writeLine([]byte(REPLY_NOT_FOUND))
	}
	var response []byte
	this.talkToBackward([]byte(fmt.Sprintf(CMD_TOUCH_FORMAT, util.IdMgr.ToHostID(gid))), hostFilter(host), simpleResponseHandler(&response, false))
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
reserve-with-timeout <seconds>\r\n

reply:
DEADLINE_SOON\r\n
TIMED_OUT\r\n
RESERVED <id> <bytes>\r\n <data>\r\n
*/
func reserveTimeoutHandler(this *ForwardConnection, cmd []byte) error {
	var response []byte
	handler := func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err != nil {
			logger.Warning("reserve-with-timeout got response error=", err.Error())
			return false
		}
		if strings.HasPrefix(string(line), REPLY_TIMED_OUT) {
			if response == nil {
				response = []byte(REPLY_TIMED_OUT)
			}
			return false
		}
		if strings.HasPrefix(string(line), REPLY_DEADLINE_SOON) {
			response = line
			return true
		}
		if strings.HasPrefix(string(line), REPLY_RESERVED) {
			var hid uint64
			var bodyLen int
			fmt.Sscanf(string(line), REPLY_RESERVED_FORMAT, &hid, &bodyLen)

			wholeresponse := util.BufferPool.Get()
			defer func() {
				util.BufferPool.Release(wholeresponse)
			}()

			wholeresponse.WriteString(fmt.Sprintf(REPLY_RESERVED_FORMAT, util.IdMgr.ToGlobalID(bwConn.HostAddr(), hid), bodyLen))
			wholeresponse.WriteString("\r\n")

			body := make([]byte, bodyLen+2) //include \r\n
			err = bwConn.read(body)
			if err != nil {
				return false
			}

			wholeresponse.Write(body[:len(body)-2])
			response = wholeresponse.Bytes()
			return true
		}
		response = line
		return true
	}

	this.talkToBackward(cmd, noSkipFilter, handler)
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
bind A B\r\n
A becomes a virtual tube, so that all the jobs will go to B

reply:
BINDED\r\n
*/
func bindHandler(this *ForwardConnection, cmd []byte) error {
	var response []byte
	this.talkToBackward(cmd, noSkipFilter, simpleResponseHandler(&response, true))
	if response != nil {
		return this.writeLine(response)
	}
	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

func unbindHandler(this *ForwardConnection, cmd []byte) error {
	var response []byte
	this.talkToBackward(cmd, noSkipFilter, simpleResponseHandler(&response, true))
	if response != nil {
		return this.writeLine(response)
	}
	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
-peek <id>\r\n

reply:
There are two possible responses, either a single line:
NOT_FOUND\r\n
FOUND <id> <bytes>\r\n <data>\r\n
*/
func peekJobHandler(this *ForwardConnection, cmd []byte) error {
	var gid uint64
	n, err := fmt.Sscanf(string(cmd), "peek %d", &gid)
	if err != nil || n != 1 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}

	host, ok := util.IdMgr.ToHost(gid)
	if !ok {
		return this.writeLine([]byte(REPLY_NOT_FOUND))
	}

	var response []byte
	handler := func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err != nil {
			return true
		}
		if strings.HasPrefix(string(line), REPLY_FOUND) {
			var bodyLen int
			var hid uint64
			fmt.Sscanf(string(line), REPLY_FOUND_FORMAT, &hid, &bodyLen)

			wholeCmd := util.BufferPool.Get()
			defer func() {
				util.BufferPool.Release(wholeCmd)
			}()

			wholeCmd.WriteString(fmt.Sprintf(REPLY_FOUND_FORMAT, gid, bodyLen))
			wholeCmd.WriteString("\r\n")
			body := make([]byte, bodyLen+2)
			err = bwConn.read(body)
			if err != nil {
				return true
			}
			wholeCmd.Write(body[:len(body)-2])
			response = wholeCmd.Bytes()
			return true
		}
		if strings.HasPrefix(string(line), REPLY_NOT_FOUND) {
			return true
		}

		return true
	}
	this.talkToBackward([]byte(fmt.Sprintf("peek %d", util.IdMgr.ToHostID(gid))), hostFilter(host), handler)
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
peek-ready\r\n 返回下一个ready job
peek-delayed\r\n 返回下一个延迟剩余时间最短的job
peek-buried\r\n 返回下一个在buried列表中的job

reply:
NOT_FOUND\r\n 如果job不存在，或者没有对应状态的job
FOUND <id> <bytes>\r\n <data>\r\n
*/
func peekGeneralHandle(this *ForwardConnection, cmd []byte) error {
	var response []byte
	handler := func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err != nil {
			return false
		}
		if strings.HasPrefix(string(line), REPLY_FOUND) {
			var bodyLen int
			var hid uint64
			fmt.Sscanf(string(line), REPLY_FOUND_FORMAT, &hid, &bodyLen)

			wholeCmd := util.BufferPool.Get()
			defer func() {
				util.BufferPool.Release(wholeCmd)
			}()

			wholeCmd.WriteString(fmt.Sprintf(REPLY_FOUND_FORMAT, util.IdMgr.ToGlobalID(bwConn.HostAddr(), hid), bodyLen))
			wholeCmd.WriteString("\r\n")
			body := make([]byte, bodyLen+2)
			err = bwConn.read(body)
			if err != nil {
				return false
			}
			wholeCmd.Write(body[:len(body)-2])
			response = wholeCmd.Bytes()
			return true
		}
		if strings.HasPrefix(string(line), REPLY_NOT_FOUND) {
			if response == nil {
				response = line
			}
			return false
		}

		return false
	}
	this.talkToBackward(cmd, noSkipFilter, handler)
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
bury <id> <pri>\r\n

reply:
BURIED\r\n 表明成功
NOT_FOUND\r\n 如果job不存在或者client没有预订此job
*/
func buryHandler(this *ForwardConnection, cmd []byte) error {
	var gid uint64
	var pri int
	n, err := fmt.Sscanf(string(cmd), CMD_BURY_FORMAT, &gid, &pri)
	if err != nil || n != 2 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}

	host, ok := util.IdMgr.ToHost(gid)
	if !ok {
		return this.writeLine([]byte(REPLY_NOT_FOUND))
	}

	var response []byte
	this.talkToBackward([]byte(fmt.Sprintf(CMD_BURY_FORMAT, util.IdMgr.ToHostID(gid), pri)), hostFilter(host), simpleResponseHandler(&response, false))
	if response != nil {
		return this.writeLine(response)
	}
	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
pause-tube <tube-name> <delay>\r\n
- <tube> is the tube to pause
- <delay> is an integer number of seconds to wait before reserving any more jobs from the queue There are two possible responses:

reply:
- "PAUSED\r\n" to indicate success.
- "NOT_FOUND\r\n" if the tube does not exist.
*/
func pauseTubeHandler(this *ForwardConnection, cmd []byte) error {
	var tube string
	var delay int
	n, err := fmt.Sscanf(string(cmd), CMD_PAUSE_TUBE_FORMAT, &tube, &delay)
	if err != nil || n != 2 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}
	var response []byte
	handler := func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err == nil {
			if string(line) == REPLY_PAUSE {
				response = line
			} else if string(line) == REPLY_NOT_FOUND {
				if response == nil {
					response = line
				}
			}
		}
		return false
	}
	this.talkToBackward(cmd, noSkipFilter, handler)
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
kick-job <id>\r\n
- <id> is the job id to kick.

The response is one of:
- "NOT_FOUND\r\n" if the job does not exist or is not in a kickable state. This can also happen upon internal errors.
- "KICKED\r\n" when the operation succeeded.
*/
func jobKickHandler(this *ForwardConnection, cmd []byte) error {
	var gid uint64
	n, err := fmt.Sscanf(string(cmd), CMD_KICK_JOB_FORMAT, &gid)
	if err != nil || n != 1 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}
	host, ok := util.IdMgr.ToHost(gid)
	if !ok {
		return this.writeLine([]byte(REPLY_NOT_FOUND))
	}
	var response []byte
	this.talkToBackward([]byte(fmt.Sprintf(CMD_KICK_JOB_FORMAT, util.IdMgr.ToHostID(gid))), hostFilter(host), simpleResponseHandler(&response, false))
	if response != nil {
		return this.writeLine(response)
	}
	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
The kick command applies only to the currently used tube. It moves jobs into the ready queue. If there are any buried jobs, it will only kick buried jobs. Otherwise it will kick delayed jobs. It looks like:
kick <bound>\r\n
- <bound> is an integer upper bound on the number of jobs to kick. The server will kick no more than <bound> jobs.

The response is of the form: KICKED <count>\r\n
- <count> is an integer indicating the number of jobs actually kicked.
func kickHandler(this *ForwardConnection, cmd []byte) error {
	var bound uint32
	n, err := fmt.Sscanf(string(cmd), "kick %d", &bound)
	if err != nil || n != 1 {
		logger.Warning(REPLY_BAD_FORMAT, ":", string(cmd))
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}
	totalKicked := uint32(0)
	handler := func(bwConn *BackwardConnection) bool {
		line, err := bwConn.readLine()
		if err != nil {
			logger.Warningf("kick response error=[%s]", err.Error())
			return false
		}
		var kickedCount uint32
		n, err = fmt.Sscanf(string(line), "KICKED %d", &kickedCount)
		if err != nil || n != 1 {
			logger.Warningf("kick response format error=[%s]", err.Error())
			return false
		}

		totalKicked += kickedCount
		return totalKicked
	}
	for {
		if totalBound == bound {
			break
		} else if !updated {
			break
		}
		updated = false
		this.talkToBackward(cmd, noSkipFilter, handler)
	}
	return this.writeLine([]byte(fmt.Sprintf("KICKED %d", totalBound)))
}
*/

/*
The list-tubes command returns a list of all existing tubes.
Its form is: list-tubes\r\n

The response is:
OK <bytes>\r\n
<data>\r\n
- <bytes> is the size of the following data section in bytes. - <data> is a sequence of bytes of length <bytes> from the previous line. It is a YAML file containing all tube names as a list of strings.
*/
func listComplexHandler(this *ForwardConnection, cmd []byte) error {
	var response []byte
	this.talkToBackward(cmd, noSkipFilter, listInfoResponseHandler(&response))
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

func listSimpleHandler(this *ForwardConnection, cmd []byte) error {
	var response []byte
	this.talkToBackward(cmd, noSkipFilter, simpleResponseHandler(&response, false))
	if response != nil {
		return this.writeLine(response)
	}
	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

/*
post:
stats-job <id>\r\n
- <id> is a job id. The response is one of:

- "NOT_FOUND\r\n" if the job does not exist.
- "OK <bytes>\r\n<data>\r\n"
*/
func jobStatesHandler(this *ForwardConnection, cmd []byte) error {
	var gid uint64
	n, err := fmt.Sscanf(string(cmd), CMD_STATS_JOB_FORMAT, &gid)
	if err != nil || n != 1 {
		return this.writeLine([]byte(REPLY_BAD_FORMAT))
	}

	host, ok := util.IdMgr.ToHost(gid)
	if !ok {
		return this.writeLine([]byte(REPLY_NOT_FOUND))
	}
	var response []byte
	this.talkToBackward([]byte(fmt.Sprintf(CMD_STATS_JOB_FORMAT, util.IdMgr.ToHostID(gid))), hostFilter(host), listInfoResponseHandler(&response))
	if response != nil {
		return this.writeLine(response)
	}

	return this.writeLine([]byte(ERR_BACKWARD_SVRS_DOWN))
}

func quitHandler(this *ForwardConnection, cmd []byte) error {
	return errors.New(CMD_QUIT)
}

func healthHandler(this *ForwardConnection, cmd []byte) error {
	return this.writeLine([]byte(util.ConnPool.HealthMessage()))
}

func unknownHandler(this *ForwardConnection, cmd []byte) error {
	return this.writeLine([]byte(REPLY_UNKNOWN_CMD))
}

//command handlers ends

func findHandler(cmdBuf []byte) cmdHandler {
	cmd := string(cmdBuf)
	if strings.HasPrefix(cmd, CMD_PUT) {
		return putHandler
	}
	if strings.HasPrefix(cmd, CMD_HEALTH) {
		return healthHandler
	}
	if strings.HasPrefix(cmd, CMD_USE) {
		return useHandler
	}
	if strings.HasPrefix(cmd, CMD_RESERVE_TIMEOUT) {
		return reserveTimeoutHandler
	}
	if strings.HasPrefix(cmd, CMD_RESERVE) {
		return reserveHandler
	}
	if strings.HasPrefix(cmd, CMD_DELETE) {
		return deleteHandler
	}
	if strings.HasPrefix(cmd, CMD_IGNORE) {
		return ignoreHandler
	}
	if strings.HasPrefix(cmd, CMD_WATCH) {
		return watchHandler
	}
	if strings.HasPrefix(cmd, CMD_RELEASE) {
		return releaseHandler
	}
	if strings.HasPrefix(cmd, CMD_QUIT) {
		return quitHandler
	}
	if strings.HasPrefix(cmd, CMD_TOUCH) {
		return touchHandler
	}
	if strings.HasPrefix(cmd, CMD_BIND) {
		return bindHandler
	}
	if strings.HasPrefix(cmd, CMD_UNBIND) {
		return unbindHandler
	}

	if strings.HasPrefix(cmd, CMD_PEEKJOB) {
		return peekJobHandler
	}

	if strings.HasPrefix(cmd, CMD_BURY) {
		return buryHandler
	}

	if strings.HasPrefix(cmd, CMD_PEEK_READY) ||
		strings.HasPrefix(cmd, CMD_PEEK_BURIED) ||
		strings.HasPrefix(cmd, CMD_PEEK_DELAYED) {
		return peekGeneralHandle
	}
	if strings.HasPrefix(cmd, CMD_PAUSE_TUBE) {
		return pauseTubeHandler
	}

	if strings.HasPrefix(cmd, CMD_JOBKICK) {
		return jobKickHandler
	}

	if strings.HasPrefix(cmd, CMD_JOBSTATS) {
		return jobStatesHandler
	}
	/*
		if strings.HasPrefix(cmd, CMD_KICK) {
			return kickHandler
		}
	*/
	if strings.HasPrefix(cmd, CMD_LIST_TUBES) ||
		strings.HasPrefix(cmd, CMD_LIST_BINDINGS) ||
		strings.HasPrefix(cmd, CMD_STATS) ||
		strings.HasPrefix(cmd, CMD_LIST_TUBES_WATCHED) ||
		strings.HasPrefix(cmd, CMD_LIST_BURIED) ||
		strings.HasPrefix(cmd, CMD_STATS_TUBE) {

		return listComplexHandler
	}
	if strings.HasPrefix(cmd, CMD_LIST_TUBE_USED) {
		return listSimpleHandler
	}

	return unknownHandler
}
