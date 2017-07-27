package main

import (
	"flag"
	"strings"

	"github.com/coreos/etcd/raft/raftpb"
)

var (
	cluster string
	id      int
	kvport  int
	join    bool
)

func init() {
	flag.StringVar(&cluster, "cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	flag.IntVar(&id, "id", 1, "node ID")
	flag.IntVar(&kvport, "port", 9121, "key-value server port")
	flag.BoolVar(&join, "json", false, "join an existing cluster")
	flag.Parse()
}

func main() {
	proposeC := make(chan string)
	defer close(proposeC)

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) {
		return kvs.getSnapshot()
	}
	commitC, errorC, snapshotterReady := newRaftNode(id, strings.Split(cluster, ","), join, getSnapshot, proposeC, confChangeC)
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)
	serveHttpKVAPI(kvs, kvport, confChangeC, errorC)
}
