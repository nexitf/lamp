package lamp_test

import (
	"fmt"
	"net/url"
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
		lamp.WithPublicOptions(1, ":3306", "mysql", lamp.DefaultWeight, "username=admin&password=123456"),
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

	closeWatch, err := lc.Watch("user-svr", func(endpoints []lamp.Endpoint, closed bool) {
		fmt.Printf("Watch: %+v %+v\n", endpoints, closed)
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
			endpoints, err := lc.DiscoverWithProtocol("user-svr", "mysql")
			if err != nil {
				fmt.Printf("Discover: %s\n", err.Error())
			} else {
				for _, endpoint := range endpoints {
					options, _ := url.ParseQuery(endpoint.Meta)
					fmt.Printf("Meta: %+v\n", options)
				}
				fmt.Printf("Discover: %+v\n", endpoints)
			}
		}
	}()

	wg.Wait()

	time.Sleep(20 * time.Second)

}
