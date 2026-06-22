package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/guacamole-operator/guacamole-operator/controllers"
	"github.com/guacamole-operator/guacamole-operator/internal/ws"
)

// id identifies a Guacamole instance by its name and namespace.
type id struct {
	Namespace string
	Name      string
}

// client encapsulates a WebSocket client and holds its relevant
// channels and context cancel functions.
type client struct {
	*ws.Client
	dataCh <-chan []byte
	errCh  <-chan error
	cancel context.CancelFunc
}

// Listener implements an event listener for CloudEvents produced by the
// custom Guacamole `cloudevents` extension.
type Listener struct {
	// WebSocket clients per Guacamole instance.
	clients map[id]*client
	mutex   sync.RWMutex
	closed  bool

	eventCh chan<- controllers.GuacamoleWrappedEvent
	errCh   chan<- error
	wg      sync.WaitGroup
}

// New creates a Listener forwarding processed events and errors to the
// provided output channels.
func New(eventCh chan<- controllers.GuacamoleWrappedEvent, errCh chan<- error) *Listener {
	return &Listener{
		clients: make(map[id]*client),
		eventCh: eventCh,
		errCh:   errCh,
	}
}

// Add a WebSocket Client for a Guacamole Instance.
func (l *Listener) Add(namespace, name, URL string) {
	id := id{
		Namespace: namespace,
		Name:      name,
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return
	}

	if _, exists := l.clients[id]; exists {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	dataCh := make(chan []byte, 1)
	errCh := make(chan error, 1)

	c := &client{
		Client: ws.New(URL),
		dataCh: dataCh,
		errCh:  errCh,
		cancel: cancel,
	}
	l.clients[id] = c

	go c.Read(ctx, dataCh, errCh)

	l.wg.Add(1)
	go l.forward(ctx, id, c)
}

// forward blocks on the client's channels and forwards processed events to the
// listener's shared output channels using context-aware sends.
func (l *Listener) forward(ctx context.Context, id id, c *client) {
	defer l.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-c.dataCh:
			e, err := newEvent(id, msg)
			if err != nil {
				select {
				case l.errCh <- fmt.Errorf("%s in %s: %w", id.Name, id.Namespace, err):
				case <-ctx.Done():
					return
				}
				continue
			}

			if e == nil {
				continue
			}

			select {
			case l.eventCh <- *e:
			case <-ctx.Done():
				return
			}

		case err := <-c.errCh:
			select {
			case l.errCh <- fmt.Errorf("%s in %s: %w", id.Name, id.Namespace, err):
			case <-ctx.Done():
				return
			}
		}
	}
}

// Remove a WebSocket client for a Guacamole instance and close the connection.
func (l *Listener) Remove(namespace, name string) {
	id := id{
		Namespace: namespace,
		Name:      name,
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	c, exists := l.clients[id]
	if !exists {
		return
	}

	c.cancel()
	c.Close()

	delete(l.clients, id)
}

// Listen blocks until the context is cancelled, then shuts down all clients
// and their forwarders before signalling completion via doneCh.
func (l *Listener) Listen(ctx context.Context, doneCh chan<- struct{}) {
	<-ctx.Done()

	l.mutex.Lock()
	l.closed = true
	for id, c := range l.clients {
		c.cancel()
		c.Close()
		delete(l.clients, id)
	}
	l.mutex.Unlock()

	l.wg.Wait()
	close(doneCh)
}

// newEvent parses a raw WebSocket message and, if it is a relevant user
// success event, returns a fully constructed GuacamoleWrappedEvent. It returns
// (nil, nil) when the message is valid but not a user success event (i.e. the
// caller should skip it) and (nil, err) on a parse failure.
func newEvent(id id, msg []byte) (*controllers.GuacamoleWrappedEvent, error) {
	ce := cloudevents.NewEvent()
	if err := json.Unmarshal(msg, &ce); err != nil {
		return nil, err
	}

	validEventTypes := []string{
		"io.github.guacamole_operator.user.success.create",
		"io.github.guacamole_operator.user.success.update",
		"io.github.guacamole_operator.user.success.delete",
	}

	if !slices.Contains(validEventTypes, ce.Context.GetType()) {
		return nil, nil
	}

	var u userData
	if err := json.Unmarshal(ce.Data(), &u); err != nil {
		return nil, err
	}

	return &controllers.GuacamoleWrappedEvent{
		Object: &event{
			namespace: id.Namespace,
			name:      id.Name,
			user:      u.Username,
		},
	}, nil
}
