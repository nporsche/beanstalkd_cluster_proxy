package util

func IsExisted(target []string, x string) bool {
	for _, ele := range target {
		if ele == x {
			return true
		}
	}
	return false
}

func AddToSlice(target *[]string, x string) {
	if !IsExisted(*target, x) {
		*target = append(*target, x)
	}
}

func RemoveFromSlice(target *[]string, x string) {
	for i, ele := range *target {
		if ele == x {
			(*target)[i], (*target)[len(*target)-1] = (*target)[len(*target)-1], (*target)[i]
			*target = (*target)[:len(*target)-1]
			break
		}
	}
}
