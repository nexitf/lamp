package lamp

import (
	"sort"
)

func Sort(addrs []Address) {
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Addr < addrs[j].Addr })
}

func SortByID(addrs []Address) {
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].ID < addrs[j].ID })
}
