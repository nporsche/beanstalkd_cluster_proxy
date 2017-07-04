package util

type IdManager struct {
	idMap   map[uint64]string
	hostMap map[string]uint64
}

const (
	CAP_POWER = 14
	CAP       = 1 << 14 //16384
)

func NewIdManager(addrs []string) *IdManager {
	this := new(IdManager)
	this.idMap = make(map[uint64]string)
	this.hostMap = make(map[string]uint64)

	for i, addr := range addrs {
		this.idMap[uint64(i)] = addr
		this.hostMap[addr] = uint64(i)
	}

	return this
}

func (this *IdManager) ToGlobalID(host string, localId uint64) uint64 {
	return localId<<CAP_POWER + this.hostMap[host]
}

func (this *IdManager) ToHost(gid uint64) (host string, ok bool) {
	host, ok = this.idMap[gid%CAP]
	return
}

func (this *IdManager) ToHostID(gid uint64) uint64 {
	return gid >> CAP_POWER
}
