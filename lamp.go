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
	ErrEndpointNotFound       = errors.New("endpoint not found")
)

const (
	DefaultTag    = "default"
	DefaultWeight = 100
)

type Middleware interface {
	Expose(ctx context.Context, serviceName string, endpoints map[string]Endpoint, ttl int64) (cancel func() error, err error)
	Discover(ctx context.Context, serviceName string, tag string) (endpoints []Endpoint, err error)
	Watch(ctx context.Context, serviceName string, tag string, update func(endpoints []Endpoint, closed bool)) (close func(), err error)
	Close() error
}

type Client struct {
	middleware Middleware
	close      func() error
}

// NewClient
// e. etcd://127.0.0.1:2379/services
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
	endpoints map[string]Endpoint
	ttl       int64
}

type ExposeOption func(*exposeOptions)

// WithTTL
func WithTTL(ttl int64) ExposeOption {
	return func(opts *exposeOptions) { opts.ttl = ttl }
}

// WithPublic
func WithPublic(addr string) ExposeOption {
	return WithPublicOptions(0, addr, DefaultTag, DefaultWeight, "")
}

// WithPublicOptions
func WithPublicOptions(id int, addr string, tag string, weight int, meta string) ExposeOption {
	return func(opts *exposeOptions) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return
		}
		if id <= 0 {
			if value := os.Getenv("LAMP_ENDPOINT_ID"); value != "" {
				id, _ = strconv.Atoi(value)
			}
		}
		if host == "" {
			host = os.Getenv("LAMP_ENDPOINT_HOSTNAME")
		}
		if host == "" || port == "" || tag == "" || weight < 0 || id < 0 {
			return
		}
		opts.endpoints[tag] = Endpoint{ID: id, Addr: host + ":" + port, Weight: weight, Meta: meta}
	}
}

// Expose
func (c *Client) Expose(serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	return c.ExposeWithContext(context.Background(), serviceName, opts...)
}

// ExposeWithContext
func (c *Client) ExposeWithContext(ctx context.Context, serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	expOpts := exposeOptions{
		endpoints: make(map[string]Endpoint),
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(&expOpts)
	}

	// Option: endpoints
	if len(expOpts.endpoints) <= 0 {
		return nil, ErrEndpointNotFound
	}

	// Option: ttl
	if expOpts.ttl <= 0 {
		expOpts.ttl = 30
	}

	return c.middleware.Expose(ctx, serviceName, expOpts.endpoints, expOpts.ttl)
}

// Discover
func (c *Client) Discover(serviceName string) (endpoints []Endpoint, err error) {
	return c.DiscoverWithContext(context.Background(), serviceName, DefaultTag)
}

// DiscoverWithTag
func (c *Client) DiscoverWithTag(serviceName string, tag string) (endpoints []Endpoint, err error) {
	return c.DiscoverWithContext(context.Background(), serviceName, tag)
}

// DiscoverWithContext
func (c *Client) DiscoverWithContext(ctx context.Context, serviceName string, tag string) (endpoints []Endpoint, err error) {
	return c.middleware.Discover(ctx, serviceName, tag)
}

// Watch
func (c *Client) Watch(serviceName string, update func(endpoints []Endpoint, closed bool)) (close func(), err error) {
	return c.WatchWithContext(context.Background(), serviceName, DefaultTag, update)
}

// WatchWithTag
func (c *Client) WatchWithTag(serviceName string, tag string, update func(endpoints []Endpoint, closed bool)) (close func(), err error) {
	return c.WatchWithContext(context.Background(), serviceName, tag, update)
}

// WatchWithContext
func (c *Client) WatchWithContext(ctx context.Context, serviceName string, tag string, update func(endpoints []Endpoint, closed bool)) (close func(), err error) {
	return c.middleware.Watch(ctx, serviceName, tag, update)
}

// Close
func (c *Client) Close() (err error) {
	return c.close()
}
