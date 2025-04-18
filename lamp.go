package lamp

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
)

var (
	ErrMiddlewareNotSupported = errors.New("middleware not supported")
	ErrAddressNotFound        = errors.New("address not found")
)

const (
	NodeProtocolDefault = "mixed"
	NodeWeightDefault   = 100
)

type Middleware interface {
	Expose(ctx context.Context, serviceName string, addrs map[string]Address, ttl int64) (cancel func() error, err error)
	Discover(ctx context.Context, serviceName string, protocol string) (addrs []Address, err error)
	Watch(ctx context.Context, serviceName string, protocol string, update func(addrs []Address, closed bool)) (close func(), err error)
	Close() error
}

type Client struct {
	middleware Middleware
	close      func() error
}

// NewClient
// e. etcd://127.0.0.1:2379/services?
func NewClient(cfg string) (c *Client, err error) {
	var URL *url.URL
	var middleware Middleware

	if URL, err = url.Parse(cfg); err != nil {
		return
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	switch URL.Scheme {
	// Use etcd
	case "etcd":
		middleware, err = newEtcdWithURL(ctx, URL)
	// Not supported
	default:
		err = ErrMiddlewareNotSupported
	}

	if err != nil {
		cancelCtx()
		return
	}

	c = &Client{middleware: middleware}
	c.close = func() (err error) {
		cancelCtx()
		return middleware.Close()
	}

	return
}

type exposeOptions struct {
	Addrs map[string]Address
	TTL   int64
}

type fnExposeOption struct {
	f func(*exposeOptions)
}

// newFnExposeOption
func newFnExposeOption(f func(*exposeOptions)) *fnExposeOption {
	return &fnExposeOption{f: f}
}

// apply
func (expOpt *fnExposeOption) apply(opts *exposeOptions) {
	expOpt.f(opts)
}

type ExposeOption interface {
	apply(opts *exposeOptions)
}

// WithTTL
func WithTTL(ttl int64) ExposeOption {
	return newFnExposeOption(func(opts *exposeOptions) { opts.TTL = ttl })
}

// WithPublic
func WithPublic(addr string) ExposeOption {
	return WithPublicOptions(0, addr, NodeProtocolDefault, NodeWeightDefault, "")
}

// WithPublicOptions
func WithPublicOptions(id int, addr string, protocol string, weight int, meta string) ExposeOption {
	return newFnExposeOption(func(opts *exposeOptions) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return
		}
		if id <= 0 {
			if value := os.Getenv("LAMP_NODE_ID"); value != "" {
				id, _ = strconv.Atoi(value)
			}
		}
		if host == "" {
			host = os.Getenv("LAMP_NODE_HOSTNAME")
		}
		if host == "" || port == "" || protocol == "" || weight < 0 || id < 0 {
			return
		}
		opts.Addrs[protocol] = Address{ID: id, Addr: host + ":" + port, Weight: weight, Meta: meta}
	})
}

// Expose
func (c *Client) Expose(serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	return c.ExposeWithContext(context.Background(), serviceName, opts...)
}

// ExposeWithContext
func (c *Client) ExposeWithContext(ctx context.Context, serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	expOpts := exposeOptions{
		Addrs: make(map[string]Address),
	}
	// Apply options
	for _, opt := range opts {
		opt.apply(&expOpts)
	}

	// Option: TTL
	if expOpts.TTL <= 0 {
		expOpts.TTL = 30
	}

	// Option: Addrs
	if len(expOpts.Addrs) <= 0 {
		return nil, ErrAddressNotFound
	}

	return c.middleware.Expose(ctx, serviceName, expOpts.Addrs, expOpts.TTL)
}

// Discover
func (c *Client) Discover(serviceName string) (addrs []Address, err error) {
	return c.DiscoverWithContext(context.Background(), serviceName, NodeProtocolDefault)
}

// DiscoverWithProtocol
func (c *Client) DiscoverWithProtocol(serviceName string, protocol string) (addrs []Address, err error) {
	return c.DiscoverWithContext(context.Background(), serviceName, protocol)
}

// DiscoverWithContext
func (c *Client) DiscoverWithContext(ctx context.Context, serviceName string, protocol string) (addrs []Address, err error) {
	return c.middleware.Discover(ctx, serviceName, protocol)
}

// Watch
func (c *Client) Watch(serviceName string, update func(addrs []Address, closed bool)) (close func(), err error) {
	return c.WatchWithContext(context.Background(), serviceName, NodeProtocolDefault, update)
}

// WatchWithProtocol
func (c *Client) WatchWithProtocol(serviceName string, protocol string, update func(addrs []Address, closed bool)) (close func(), err error) {
	return c.WatchWithContext(context.Background(), serviceName, protocol, update)
}

// WatchWithContext
func (c *Client) WatchWithContext(ctx context.Context, serviceName string, protocol string, update func(addrs []Address, closed bool)) (close func(), err error) {
	return c.middleware.Watch(ctx, serviceName, protocol, update)
}

// Close
func (c *Client) Close() (err error) {
	return c.close()
}
