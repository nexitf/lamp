package lamp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	client "go.etcd.io/etcd/client/v3"
)

type etcdConfig struct {
	client.Config
	Namespace string
}

type etcdClient struct {
	cfg etcdConfig
	cli *client.Client
}

var (
	ErrInvalidEndpoint = errors.New("invalid endpoint")
	ErrInvalidKey      = errors.New("invalid key")
	ErrInvalidAddress  = errors.New("invalid address")
	ErrRegisterFailed  = errors.New("register failed")
)

// newEtcd
func newEtcd(ctx context.Context, cfg etcdConfig) (c *etcdClient, err error) {
	cfg.Context = ctx

	cli, err := client.New(cfg.Config)
	if err != nil {
		return nil, err
	}
	c = &etcdClient{cfg: cfg, cli: cli}
	return
}

// newEtcdWithURL
func newEtcdWithURL(ctx context.Context, URL *url.URL) (c *etcdClient, err error) {
	var cfg etcdConfig

	// Option: Endpoints
	if URL.Host == "" {
		return nil, ErrInvalidEndpoint
	}
	for _, endpoint := range strings.Split(URL.Host, ",") {
		if _, _, err = net.SplitHostPort(endpoint); err != nil {
			return nil, err
		}
		cfg.Endpoints = append(cfg.Endpoints, endpoint)
	}

	// Option: Namespace
	if URL.Path != "" && URL.Path != "/" {
		cfg.Namespace = URL.Path
	}

	return newEtcd(ctx, cfg)
}

// Expose
func (c *etcdClient) Expose(ctx context.Context, serviceName string, addrs map[string]Address, ttl int64) (cancel func() error, err error) {
	if len(addrs) <= 0 {
		return nil, ErrInvalidAddress
	}

	leaseResp, err := c.cli.Grant(ctx, int64(ttl))
	if err != nil {
		return nil, err
	}

	keepAliveChan, err := c.cli.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return nil, err
	}

	ctx, cancelCtx := context.WithCancel(ctx)

	cancel = func() (err error) {
		defer cancelCtx()
		_, err = c.cli.Revoke(ctx, leaseResp.ID)
		return
	}

	var wg sync.WaitGroup
	var now = time.Now()
	var ops = make([]client.Op, 0, len(addrs))
	var servicePrefix = c.servicePrefix(serviceName)

	wg.Add(1)
	go func() {
		wg.Done()
		defer cancel()
		for {
			select {
			// KeepAlive channel
			case _, ok := <-keepAliveChan:
				if !ok {
					return
				}
			// Cancel
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	for protocol, addr := range addrs {
		node := Node{
			ID:     addr.ID,
			Addr:   addr.Addr,
			Weight: addr.Weight,
			Meta:   addr.Meta,
			Time:   now.Unix(),
		}
		info, err := json.Marshal(node)
		if err != nil {
			cancel()
			return nil, err
		}
		ops = append(ops,
			client.OpPut(
				servicePrefix+"/"+protocol+"/"+generateNodeID(addr.Addr),
				string(info),
				client.WithLease(leaseResp.ID),
			))
	}

	txnResp, err := c.cli.Txn(ctx).If().Then(ops...).Commit()
	if err != nil {
		defer cancel()
		return nil, err
	}

	if !txnResp.Succeeded {
		defer cancel()
		return nil, ErrRegisterFailed
	}

	return
}

// Discover
func (c *etcdClient) Discover(ctx context.Context, serviceName string, protocol string) (addrs []Address, err error) {
	servicePrefix := c.servicePrefix(serviceName)

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	serviceAddrs := make(map[string]map[string]Node)

	for _, kv := range getResp.Kvs {
		p, id, node, err := c.isValidNode(servicePrefix, string(kv.Key), kv.Value)
		if err != nil {
			continue
		}
		if _, ok := serviceAddrs[p]; !ok {
			serviceAddrs[p] = map[string]Node{id: node}
		} else {
			serviceAddrs[p][id] = node
		}
	}

	return c.selectAddrs(serviceAddrs, protocol), nil
}

// Watch
func (c *etcdClient) Watch(ctx context.Context, serviceName string, protocol string, update func(addrs []Address, closed bool)) (close func(), err error) {
	var wg sync.WaitGroup
	var serviceAddrs = make(map[string]map[string]Node)
	var servicePrefix = c.servicePrefix(serviceName)

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range getResp.Kvs {
		p, id, node, err := c.isValidNode(servicePrefix, string(kv.Key), kv.Value)
		if err != nil {
			continue
		}
		if _, ok := serviceAddrs[p]; !ok {
			serviceAddrs[p] = map[string]Node{id: node}
		} else {
			serviceAddrs[p][id] = node
		}
	}

	ctx, close = context.WithCancel(ctx)
	watchChan := c.cli.Watch(ctx, servicePrefix, client.WithPrefix())

	wg.Add(1)
	go func() {
		wg.Done()
		c.watch(servicePrefix, protocol, update, serviceAddrs, watchChan)
	}()

	wg.Wait()

	return
}

// watch
func (c *etcdClient) watch(servicePrefix, protocol string, update func(addrs []Address, closed bool),
	serviceAddrs map[string]map[string]Node, watchChan client.WatchChan) {
	defer update(nil, true)

	// send the first notification
	update(c.selectAddrs(serviceAddrs, protocol), false)

	put := func(key string, value []byte) {
		p, id, node, err := c.isValidNode(servicePrefix, key, value)
		if err != nil {
			return
		}
		if _, ok := serviceAddrs[p]; !ok {
			serviceAddrs[p] = map[string]Node{id: node}
		} else {
			serviceAddrs[p][id] = node
		}
	}

	rem := func(key string, value []byte) {
		p, id, _, err := c.isValidNode(servicePrefix, key, value)
		if err != nil {
			return
		}
		if _, ok := serviceAddrs[p]; ok {
			delete(serviceAddrs[p], id)
			if len(serviceAddrs[p]) <= 0 {
				delete(serviceAddrs, p)
			}
		}
	}

	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case client.EventTypePut:
				put(string(event.Kv.Key), event.Kv.Value)
			case client.EventTypeDelete:
				rem(string(event.Kv.Key), event.Kv.Value)
			}
		}
		// notify
		update(c.selectAddrs(serviceAddrs, protocol), false)
	}
}

// isValidNode
func (c *etcdClient) isValidNode(servicePrefix, key string, value []byte) (protocol string, nodeID string, node Node, err error) {
	protocol, nodeID, ok := c.splitProtocolAndNodeID(key, servicePrefix)
	if !ok {
		return "", "", Node{}, ErrInvalidKey
	}
	if err = json.Unmarshal(value, &node); err != nil {
		return "", "", Node{}, err
	}
	return
}

// splitProtocolAndNodeID
func (c *etcdClient) splitProtocolAndNodeID(key, servicePrefix string) (protocol string, nodeID string, ok bool) {
	pn := strings.TrimPrefix(key, servicePrefix+"/")
	if i := strings.Index(pn, "/"); i == -1 {
		return "", "", false
	} else {
		return pn[0:i], pn[i+1:], true
	}
}

func (c *etcdClient) selectAddrs(serviceAddrs map[string]map[string]Node, protocol string) (addrs []Address) {
	if _, ok := serviceAddrs[protocol]; !ok {
		return
	}
	if protocolAddrs, ok := serviceAddrs[protocol]; ok {
		for _, addr := range protocolAddrs {
			addrs = append(addrs, Address{ID: addr.ID, Addr: addr.Addr, Weight: addr.Weight, Meta: addr.Meta})
		}
	}
	return
}

// servicePrefix
func (c *etcdClient) servicePrefix(serviceName string) string {
	return c.cfg.Namespace + "/" + serviceName
}

// Close shuts down the client's etcd connections.
func (c *etcdClient) Close() error {
	return c.cli.Close()
}
