package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/public/service"
)

func natsKVCacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Cache key/values in a NATS key-value bucket.").
		Description(connectionNameDescription() + authDescription()).
		Fields(Docs("KV")...)
}

func init() {
	err := service.RegisterCache(
		"nats_kv", natsKVCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newKVCache(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

type kvCache struct {
	connDetails connectionDetails
	bucket      string

	log *service.Logger

	shutSig *shutdown.Signaller

	connMut  sync.RWMutex
	natsConn *nats.Conn
	kv       nats.KeyValue
}

func newKVCache(conf *service.ParsedConfig, mgr *service.Resources) (*kvCache, error) {
	p := &kvCache{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if p.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if p.bucket, err = conf.FieldString(kvFieldBucket); err != nil {
		return nil, err
	}

	err = p.connect(context.Background())
	return p, err
}

func (p *kvCache) disconnect() {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		p.natsConn.Close()
		p.natsConn = nil
	}
	p.kv = nil
}

func (p *kvCache) connect(ctx context.Context) error {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		return nil
	}

	var err error
	if p.natsConn, err = p.connDetails.get(ctx); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			p.natsConn.Close()
			p.natsConn = nil
		}
	}()

	var js nats.JetStreamContext
	if js, err = p.natsConn.JetStream(); err != nil {
		return err
	}

	if p.kv, err = js.KeyValue(p.bucket); err != nil {
		return err
	}
	return nil
}

func (p *kvCache) getKV(ctx context.Context) (nats.KeyValue, error) {
	if err := p.connect(ctx); err != nil {
		return nil, err
	}
	p.connMut.RLock()
	kv := p.kv
	p.connMut.RUnlock()
	if kv == nil {
		return nil, errors.New("nats kv cache not connected")
	}
	return kv, nil
}

func (p *kvCache) Get(ctx context.Context, key string) ([]byte, error) {
	kv, err := p.getKV(ctx)
	if err != nil {
		return nil, err
	}

	entry, err := kv.Get(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil, service.ErrKeyNotFound
		}
		p.disconnect()
		return nil, err
	}
	return entry.Value(), nil
}

func (p *kvCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	kv, err := p.getKV(ctx)
	if err != nil {
		return err
	}

	_, err = kv.Put(key, value)
	if err != nil {
		p.disconnect()
	}
	return err
}

func (p *kvCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	kv, err := p.getKV(ctx)
	if err != nil {
		return err
	}

	_, err = kv.Create(key, value)
	if errors.Is(err, nats.ErrKeyExists) {
		return service.ErrKeyAlreadyExists
	}
	if err != nil {
		p.disconnect()
	}
	return err
}

func (p *kvCache) Delete(ctx context.Context, key string) error {
	kv, err := p.getKV(ctx)
	if err != nil {
		return err
	}

	err = kv.Delete(key)
	if err != nil {
		p.disconnect()
	}
	return err
}

func (p *kvCache) Close(ctx context.Context) error {
	go func() {
		p.disconnect()
		p.shutSig.TriggerHasStopped()
	}()
	select {
	case <-p.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
