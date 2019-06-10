package statebasedval

import (
	"fmt"
	"sync"
)

var (
	invalid int
	total   int
	m       sync.RWMutex
	printed bool
)

func RecordTx(isValid bool) {
	m.Lock()
	if !isValid {
		invalid++
	}
	total++
	m.Unlock()
}

func PrintRate() {
	if !printed {
		fmt.Printf("txval,%d,%d,%f\n", total, invalid, float64(invalid)/float64(10000))
	}
	printed = true
}

func GetCount() int {
	m.RLock()
	i := total
	m.RUnlock()
	return i
}
