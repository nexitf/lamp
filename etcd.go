package lamp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strconv"
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
	ErrInvalidEtcdEndpoint     = errors.New("invalid etcd endpoint")
	ErrEndpointsAreUnreachable = errors.New("endpoints are unreachable")
	ErrInvalidKey              = errors.New("invalid key")
	ErrInvalidValue            = errors.New("invalid value")
	ErrInvalidExposedEndpoint  = errors.New("invalid exposed endpoint")
	ErrRegisterFailed          = errors.New("register failed")
)

// newEtcd
func newEtcd(ctx context.Context, cfg etcdConfig) (c *etcdClient, err error) {
	cfg.Context = ctx

	cli, err := client.New(cfg.Config)
	if err != nil {
		return nil, err
	}

	health := false
	for _, endpoint := range cfg.Endpoints {
		dialCtx, cancelCtx := context.WithTimeout(ctx, cfg.DialTimeout)
		if _, err = cli.Status(dialCtx, endpoint); err == nil {
			health = true
		}
		cancelCtx()
		// At least one endpoint is healthy
		if health {
			break
		}
	}
	if !health {
		return nil, ErrEndpointsAreUnreachable
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
	if URL.Path != "" {
		cfg.Namespace = strings.TrimSuffix(URL.Path, "/")
	}
	// Option: Username
	if user := URL.User; user != nil {
		cfg.Username = user.Username()
		if password, ok := user.Password(); ok {
			cfg.Password = password
		}
	}

	params := URL.Query()
	// Option: DialTimeout
	if value := params.Get("dial-timeout"); value != "" {
		if timeout, _ := strconv.ParseInt(value, 10, 64); timeout > 0 {
			cfg.DialTimeout = time.Duration(timeout) * time.Second
		}
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
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

	for tag, endpoint := range endpoints {
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
				servicePrefix+"/"+tag+"/"+generateNodeID(endpoint.Addr),
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
func (c *etcdClient) Discover(ctx context.Context, serviceName string, tag string) (endpoints []Endpoint, err error) {
	servicePrefix := c.servicePrefix(serviceName)

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	serviceNodes := make(map[string]map[string]Node)

	for _, kv := range getResp.Kvs {
		t, id, err := c.isValidKey(servicePrefix, string(kv.Key))
		if err != nil {
			continue
		}
		node, err := c.isValidNode(kv.Value)
		if err != nil {
			continue
		}
		if _, ok := serviceNodes[t]; !ok {
			serviceNodes[t] = map[string]Node{id: node}
		} else {
			serviceNodes[t][id] = node
		}
	}

	return c.selectEndpoints(serviceNodes, tag), nil
}

// Watch
func (c *etcdClient) Watch(ctx context.Context, serviceName string, tag string, update func(endpoints []Endpoint, closed bool)) (close func(), err error) {
	var wg sync.WaitGroup
	var serviceNodes = make(map[string]map[string]Node)
	var servicePrefix = c.servicePrefix(serviceName)

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range getResp.Kvs {
		t, id, err := c.isValidKey(servicePrefix, string(kv.Key))
		if err != nil {
			continue
		}
		node, err := c.isValidNode(kv.Value)
		if err != nil {
			continue
		}
		if _, ok := serviceNodes[t]; !ok {
			serviceNodes[t] = map[string]Node{id: node}
		} else {
			serviceNodes[t][id] = node
		}
	}

	ctx, close = context.WithCancel(ctx)
	watchChan := c.cli.Watch(ctx, servicePrefix, client.WithPrefix())

	wg.Add(1)
	go func() {
		wg.Done()
		c.watch(servicePrefix, tag, update, serviceNodes, watchChan)
	}()

	wg.Wait()

	return
}

// watch
func (c *etcdClient) watch(servicePrefix, tag string, update func(endpoints []Endpoint, closed bool),
	serviceNodes map[string]map[string]Node, watchChan client.WatchChan) {
	defer update(nil, true)

	// send the first notification
	update(c.selectEndpoints(serviceNodes, tag), false)

	put := func(key string, value []byte) {
		t, id, err := c.isValidKey(servicePrefix, key)
		if err != nil {
			return
		}
		node, err := c.isValidNode(value)
		if err != nil {
			return
		}
		if _, ok := serviceNodes[t]; !ok {
			serviceNodes[t] = map[string]Node{id: node}
		} else {
			serviceNodes[t][id] = node
		}
	}

	rem := func(key string, _ []byte) {
		t, id, err := c.isValidKey(servicePrefix, key)
		if err != nil {
			return
		}
		if _, ok := serviceNodes[t]; ok {
			delete(serviceNodes[t], id)
			if len(serviceNodes[t]) <= 0 {
				delete(serviceNodes, t)
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
		update(c.selectEndpoints(serviceNodes, tag), false)
	}
}

// isValidKey
func (c *etcdClient) isValidKey(servicePrefix, key string) (tag string, nodeID string, err error) {
	tag, nodeID, ok := c.splitTagAndNodeID(key, servicePrefix)
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

// splitTagAndNodeID
func (c *etcdClient) splitTagAndNodeID(key, servicePrefix string) (tag string, nodeID string, ok bool) {
	tn := strings.TrimPrefix(key, servicePrefix+"/")
	if i := strings.Index(tn, "/"); i == -1 {
		return "", "", false
	} else {
		return tn[0:i], tn[i+1:], true
	}
}

func (c *etcdClient) selectEndpoints(serviceNodes map[string]map[string]Node, tag string) (endpoints []Endpoint) {
	if _, ok := serviceNodes[tag]; !ok {
		return
	}
	if tagNodes, ok := serviceNodes[tag]; ok {
		for _, node := range tagNodes {
			endpoints = append(endpoints, Endpoint{
				ID:     node.ID,
				Addr:   node.Addr,
				Weight: node.Weight,
				Meta:   node.Meta,
			})
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
