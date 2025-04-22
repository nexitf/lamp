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
	ErrInvalidEtcdEndpoint    = errors.New("invalid etcd endpoint")
	ErrInvalidKey             = errors.New("invalid key")
	ErrInvalidValue           = errors.New("invalid value")
	ErrInvalidExposedEndpoint = errors.New("invalid exposed endpoint")
	ErrRegisterFailed         = errors.New("register failed")
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
		return nil, ErrInvalidEtcdEndpoint
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
func (c *etcdClient) Expose(ctx context.Context, serviceName string, endpoints map[string]Endpoint, ttl int64) (cancel func() error, err error) {
	if len(endpoints) <= 0 {
		return nil, ErrInvalidExposedEndpoint
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
	var ops = make([]client.Op, 0, len(endpoints))
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

	for protocol, endpoint := range endpoints {
		node := Node{
			ID:     endpoint.ID,
			Addr:   endpoint.Addr,
			Weight: endpoint.Weight,
			Meta:   endpoint.Meta,
			Time:   now.Unix(),
		}
		info, err := json.Marshal(node)
		if err != nil {
			cancel()
			return nil, err
		}
		ops = append(ops,
			client.OpPut(
				servicePrefix+"/"+protocol+"/"+generateNodeID(endpoint.Addr),
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
func (c *etcdClient) Discover(ctx context.Context, serviceName string, protocol string) (endpoints []Endpoint, err error) {
	servicePrefix := c.servicePrefix(serviceName)

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	serviceNodes := make(map[string]map[string]Node)

	for _, kv := range getResp.Kvs {
		p, id, err := c.isValidKey(servicePrefix, string(kv.Key))
		if err != nil {
			continue
		}
		node, err := c.isValidNode(kv.Value)
		if err != nil {
			continue
		}
		if _, ok := serviceNodes[p]; !ok {
			serviceNodes[p] = map[string]Node{id: node}
		} else {
			serviceNodes[p][id] = node
		}
	}

	return c.selectEndpoints(serviceNodes, protocol), nil
}

// Watch
func (c *etcdClient) Watch(ctx context.Context, serviceName string, protocol string, update func(endpoints []Endpoint, closed bool)) (close func(), err error) {
	var wg sync.WaitGroup
	var serviceNodes = make(map[string]map[string]Node)
	var servicePrefix = c.servicePrefix(serviceName)

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range getResp.Kvs {
		p, id, err := c.isValidKey(servicePrefix, string(kv.Key))
		if err != nil {
			continue
		}
		node, err := c.isValidNode(kv.Value)
		if err != nil {
			continue
		}
		if _, ok := serviceNodes[p]; !ok {
			serviceNodes[p] = map[string]Node{id: node}
		} else {
			serviceNodes[p][id] = node
		}
	}

	ctx, close = context.WithCancel(ctx)
	watchChan := c.cli.Watch(ctx, servicePrefix, client.WithPrefix())

	wg.Add(1)
	go func() {
		wg.Done()
		c.watch(servicePrefix, protocol, update, serviceNodes, watchChan)
	}()

	wg.Wait()

	return
}

// watch
func (c *etcdClient) watch(servicePrefix, protocol string, update func(endpoints []Endpoint, closed bool),
	serviceNodes map[string]map[string]Node, watchChan client.WatchChan) {
	defer update(nil, true)

	// send the first notification
	update(c.selectEndpoints(serviceNodes, protocol), false)

	put := func(key string, value []byte) {
		p, id, err := c.isValidKey(servicePrefix, key)
		if err != nil {
			return
		}
		node, err := c.isValidNode(value)
		if err != nil {
			return
		}
		if _, ok := serviceNodes[p]; !ok {
			serviceNodes[p] = map[string]Node{id: node}
		} else {
			serviceNodes[p][id] = node
		}
	}

	rem := func(key string, _ []byte) {
		p, id, err := c.isValidKey(servicePrefix, key)
		if err != nil {
			return
		}
		if _, ok := serviceNodes[p]; ok {
			delete(serviceNodes[p], id)
			if len(serviceNodes[p]) <= 0 {
				delete(serviceNodes, p)
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
		update(c.selectEndpoints(serviceNodes, protocol), false)
	}
}

// isValidKey
func (c *etcdClient) isValidKey(servicePrefix, key string) (protocol string, nodeID string, err error) {
	protocol, nodeID, ok := c.splitProtocolAndNodeID(key, servicePrefix)
	if !ok {
		err = ErrInvalidKey
	}
	return
}

// isValidNode
func (c *etcdClient) isValidNode(value []byte) (node Node, err error) {
	if err = json.Unmarshal(value, &node); err != nil {
		return Node{}, ErrInvalidValue
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

func (c *etcdClient) selectEndpoints(serviceNodes map[string]map[string]Node, protocol string) (endpoints []Endpoint) {
	if _, ok := serviceNodes[protocol]; !ok {
		return
	}
	if protocolNodes, ok := serviceNodes[protocol]; ok {
		for _, node := range protocolNodes {
			endpoints = append(endpoints, Endpoint{ID: node.ID, Addr: node.Addr, Weight: node.Weight, Meta: node.Meta})
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
