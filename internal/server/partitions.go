package server

import (
	"errors"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"sync"

	"github.com/Chadi00/tinyq/internal/queue"
)

var partitionName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

func ValidPartition(name string) bool { return partitionName.MatchString(name) }

type partition struct {
	q       *queue.Queue
	handler http.Handler
}

type Partitions struct {
	http.Handler
	mu       sync.Mutex
	open     map[string]partition
	path     string
	maxBytes int
}

func Open(path string, maxBytes int) (*Partitions, error) {
	p := &Partitions{path: path, maxBytes: maxBytes, open: make(map[string]partition)}
	root, err := p.load("default")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/", root.handler)
	mux.HandleFunc("/partitions/{name}/", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if !ValidPartition(name) {
			http.Error(w, "invalid partition name", http.StatusBadRequest)
			return
		}
		part, err := p.load(name)
		if err != nil {
			http.Error(w, queue.ErrPersistence.Error(), http.StatusServiceUnavailable)
			return
		}
		http.StripPrefix("/partitions/"+name, part.handler).ServeHTTP(w, r)
	})
	p.Handler = mux
	return p, nil
}

func (p *Partitions) load(name string) (partition, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if part, ok := p.open[name]; ok {
		return part, nil
	}
	path := p.path
	if name != "default" {
		path += "." + name + ".wal"
	}
	q, err := queue.Open(path, p.maxBytes)
	if err != nil {
		return partition{}, err
	}
	part := partition{q, New(q)}
	p.open[name] = part
	return part, nil
}

func (p *Partitions) all() []partition {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Collect(maps.Values(p.open))
}

func (p *Partitions) Maintain() error {
	var err error
	for _, part := range p.all() {
		part.q.Expire()
		err = errors.Join(err, part.q.Compact())
	}
	return err
}

func (p *Partitions) Close() error {
	var err error
	for _, part := range p.all() {
		err = errors.Join(err, part.q.Close())
	}
	return err
}
