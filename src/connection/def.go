package connection

const (
	WANT_CMD = iota
)

const (
	LINE_BUF_SIZE = 208
)

const (
	ERR_BACKWARD_SVRS_DOWN = "BACKWARD_SVRS_DOWN"
	ERR_UNEXPECTED_ECHO    = "UNEXPECTED_ECHO"
)

var PING = "list-tube-used"
var CMD_USE_DIDI_CONFIG = "use _didi_config_"

const (
	REPLY_TIMED_OUT     = "TIMED_OUT"
	REPLY_DEADLINE_SOON = "DEADLINE_SOON"
	REPLY_RESERVED      = "RESERVED "
	REPLY_USING         = "USING "
	REPLY_INSERTED      = "INSERTED "
	REPLY_WATCHING      = "WATCHING"
	REPLY_NOT_IGNORED   = "NOT_IGNORED"
	REPLY_FOUND         = "FOUND "
	REPLY_NOT_FOUND     = "NOT_FOUND"
	REPLY_BAD_FORMAT    = "BAD_FORMAT"
	REPLY_UNKNOWN_CMD   = "UNKNOWN_COMMAND"
	REPLY_PAUSE         = "PAUSED"
	REPLY_KICKED        = "KICKED "
)

const (
	CMD_PUT                = "put "
	CMD_PEEKJOB            = "peek "
	CMD_PEEK_READY         = "peek-ready"
	CMD_PEEK_DELAYED       = "peek-delayed"
	CMD_PEEK_BURIED        = "peek-buried"
	CMD_RESERVE            = "reserve"
	CMD_RESERVE_TIMEOUT    = "reserve-with-timeout "
	CMD_DELETE             = "delete "
	CMD_RELEASE            = "release "
	CMD_BURY               = "bury "
	CMD_KICK               = "kick "
	CMD_JOBKICK            = "kick-job "
	CMD_TOUCH              = "touch "
	CMD_STATS              = "stats"
	CMD_JOBSTATS           = "stats-job "
	CMD_USE                = "use "
	CMD_WATCH              = "watch "
	CMD_IGNORE             = "ignore "
	CMD_LIST_TUBES         = "list-tubes"
	CMD_LIST_TUBE_USED     = "list-tube-used"
	CMD_LIST_TUBES_WATCHED = "list-tubes-watched"
	CMD_STATS_TUBE         = "stats-tube "
	CMD_QUIT               = "quit"
	CMD_PAUSE_TUBE         = "pause-tube"
	CMD_BIND               = "bind"
	CMD_UNBIND             = "unbind"
	CMD_LIST_BINDINGS      = "list-bindings"
	CMD_LIST_BURIED        = "list-buried"
	CMD_HEALTH             = "health"
	CMD_LOAD_CONFIG        = "load-config"
)

const (
	REPLY_INSERTED_FORMAT = "INSERTED %d"
	REPLY_RESERVED_FORMAT = "RESERVED %d %d"
	REPLY_FOUND_FORMAT    = "FOUND %d %d"
	REPLY_USING_FORMAT    = "USING %s"
	REPLY_WATCHING_FORMAT = "WATCHING %d"
)

const (
	CMD_PUT_FORMAT        = "put %d %d %d %d"
	CMD_USE_FORMAT        = "use %s"
	CMD_WATCH_FORMAT      = "watch %s"
	CMD_IGNORE_FORMAT     = "ignore %s"
	CMD_DELETE_FORMAT     = "delete %d"
	CMD_RELEASE_FORMAT    = "release %d %d %d"
	CMD_TOUCH_FORMAT      = "touch %d"
	CMD_BURY_FORMAT       = "bury %d %d"
	CMD_PAUSE_TUBE_FORMAT = "pause-tube %s %d"
	CMD_KICK_JOB_FORMAT   = "kick-job %d"
	CMD_STATS_JOB_FORMAT  = "stats-job %d"
)

/*
const (
	OP_UNKNOWN            = 0
	OP_PUT                = 1
	OP_PEEKJOB            = 2
	OP_RESERVE            = 3
	OP_DELETE             = 4
	OP_RELEASE            = 5
	OP_BURY               = 6
	OP_KICK               = 7
	OP_STATS              = 8
	OP_JOBSTATS           = 9
	OP_PEEK_BURIED        = 10
	OP_USE                = 11
	OP_WATCH              = 12
	OP_IGNORE             = 13
	OP_LIST_TUBES         = 14
	OP_LIST_TUBE_USED     = 15
	OP_LIST_TUBES_WATCHED = 16
	OP_STATS_TUBE         = 17
	OP_PEEK_READY         = 18
	OP_PEEK_DELAYED       = 19
	OP_RESERVE_TIMEOUT    = 20
	OP_TOUCH              = 21
	OP_QUIT               = 22
	OP_PAUSE_TUBE         = 23
	OP_JOBKICK            = 24
	OP_BIND               = 25
	OP_UNBIND             = 26
	OP_LIST_BINDINGS      = 27
	OP_LIST_BURIED        = 28
	OP_HEALTH             = 29
)
*/
