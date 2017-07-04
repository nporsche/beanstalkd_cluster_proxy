package util

import "fmt"
import "testing"

func Test_addToSlice(*testing.T) {
	x := []string{"a", "b", "c"}
	AddToSlice(&x, "d")
	AddToSlice(&x, "a")
	fmt.Println("should be:a b c d\r\n")
	fmt.Printf("%v\r\n", x)

	RemoveFromSlice(&x, "a")
	RemoveFromSlice(&x, "xa")
	fmt.Println("should be:b c d\r\n")
	fmt.Printf("%v\r\n", x)
}

func Test_idManager(t *testing.T) {
	idMgr := NewIdManager([]string{"a", "b", "c"})
	gid_a := idMgr.ToGlobalID("a", 0)
	gid_b := idMgr.ToGlobalID("b", 0)

	if gid_a != 0 || gid_b != 1 {
		t.Error("case 0 error")
	}

	hid_a := idMgr.ToHostID(0)
	h_a, _ := idMgr.ToHost(0)
	hid_b := idMgr.ToHostID(1)
	h_b, _ := idMgr.ToHost(1)
	if hid_a != 0 || h_a != "a" || hid_b != 0 || h_b != "b" {
		t.Error("to host case 0 error")
	}

	gid_a = idMgr.ToGlobalID("a", 1)
	gid_b = idMgr.ToGlobalID("b", 1)

	if gid_a != 16384 || gid_b != 16385 {
		t.Error("case 1 error")
	}

	hid_a = idMgr.ToHostID(16384)
	h_a, _ = idMgr.ToHost(16384)
	fmt.Println(h_a)
	hid_b = idMgr.ToHostID(16385)
	h_b, _ = idMgr.ToHost(16385)
	if hid_a != 1 || h_a != "a" || hid_b != 1 || h_b != "b" {
		t.Error("to host case 1 error")
	}
}
