package main

import (
	"fmt"
	"log"
	"os"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

type raftNode struct {
	proposeC         <-chan string
	confChangeC      <-chan raftpb.ConfChange
	commitC          chan<- *string
	errorC           chan<- error
	id               int
	peers            []string
	join             bool
	waldir           string
	snapdir          string
	getSnapshot      func() ([]byte, error)
	lastIndex        uint64
	confState        raftpb.ConfState
	snapshotIndex    uint64
	appliedIndex     uint64
	node             raft.Node
	raftStorage      *raft.MemoryStorage
	wal              *wal.WAL
	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter
	snapCount        uint64
	transport        *rafthttp.Transport
	stopc            chan struct{}
	httpstopc        chan struct{}
	httpdonec        chan struct{}
}

var defaultSnapCount uint64 = 10000

func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {
	commitC := make(chan *string)
	errorC := make(chan error)
	rn := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("dskv-%d", id),
		snapdir:     fmt.Sprintf("dskv-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
	}
	go rn.startRaft()
	return commitC, errorC, rn.snapshotterReady
}

func (rn *raftNode) startRaft() {
	if !fileutil.Exist(rn.snapdir) {
		if err := os.Mkdir(rn.snapdir, 0750); err != nil {
			log.Fatalf("dskv: cannot create dir for snapshot (%v)", err)
		}
	}
	rn.snapshotter = snap.New(rn.snapdir)
	rn.snapshotterReady <- rn.snapshotter
	oldwal := wal.Exist(rn.waldir)
	rn.wal = rn.replayWAL()
	rpeers := make([]raft.Peer, len(rn.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:              uint64(rn.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		rn.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rn.join {
			startPeers = nil
		}
		rn.node = raft.StartNode(c, startPeers)
	}
	// TODO: transport
}

func (rn *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rn.id)
	snapshot := rn.loadSnapshot()
	w := rn.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("dskv: failed to read WAL (%v)", err)
	}
	rn.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rn.raftStorage.ApplySnapshot(*snapshot)
	}
	rn.raftStorage.SetHardState(st)

	rn.raftStorage.Append(ents)
	if len(ents) > 0 {
		rn.lastIndex = ents[len(ents)-1].Index
	} else {
		rn.commitC <- nil
	}
	return w
}

func (rn *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rn.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("dskv: error loading snapshot (%v)", err)
	}
	return snapshot
}

func (rn *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rn.waldir) {
		if err := os.Mkdir(rn.waldir, 0750); err != nil {
			log.Fatalf("dskv: cannot create dir for wal(%v)", err)
		}
		w, err := wal.Create(rn.waldir, nil)
		if err != nil {
			log.Fatalf("dskv: create wal error (%v)", err)
		}
		w.Close()
	}
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(rn.waldir, walsnap)
	if err != nil {
		log.Fatalf("dskv: error loading wal (%v)", err)
	}
	return w
}
