package lamp

import (
	"sort"
)

func Sort(endpoints []Endpoint) {
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Addr < endpoints[j].Addr })
}

func SortByID(endpoints []Endpoint) {
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].ID < endpoints[j].ID })
}
