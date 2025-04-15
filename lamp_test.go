package lamp_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nexitf/lamp"
)

func TestXxx(t *testing.T) {
	os.Setenv("LAMP_NODE_HOSTNAME", "127.0.0.1")

	lc, err := lamp.NewClient("etcd://127.0.0.1:2379/services")
	if err != nil {
		t.Errorf("lamp.Init: %s", err.Error())
		return
	}
	defer lc.Close()

	cancel, err := lc.Expose("user-svr",
		lamp.WithTTL(5),
		lamp.WithPublic(":8999"),
		lamp.WithPublicOptions(1, ":80", "http", lamp.NodeWeightDefault),
	)
	if err != nil {
		t.Errorf("lamp.Expose: %s", err.Error())
		return
	}
	defer cancel()

	cancel2, err := lc.Expose("user-svr",
		lamp.WithPublic(":8990"),
	)
	if err != nil {
		t.Errorf("lamp.Expose: %s", err.Error())
		return
	}
	defer cancel2()

	closeWatch, err := lc.Watch("user-svr", func(addrs []lamp.Address, closed bool) {
		fmt.Printf("Watch: %+v %+v\n", addrs, closed)
	})
	if err != nil {
		t.Errorf("lamp.Watch: %s", err.Error())
		return
	}
	defer closeWatch()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()

		for range time.Tick(time.Second * 2) {
			addrs, err := lc.Discover("user-svr")
			if err != nil {
				fmt.Printf("Discover: %s\n", err.Error())
			} else {
				fmt.Printf("Discover: %+v\n", addrs)
			}
		}
	}()

	wg.Wait()

	time.Sleep(20 * time.Second)

}
