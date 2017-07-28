package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"golang.org/x/net/context"
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

	rn.transport = &rafthttp.Transport{
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rn.id)),
		ErrorC:      make(chan error),
	}
	rn.transport.Start()
	for i := range rn.peers {
		if i+1 != rn.id {
			rn.transport.AddPeer(types.ID(i+1), []string{rn.peers[i]})
		}
	}
	go rn.serveRaft()
	go rn.serveChannels()
}

func (rn *raftNode) serveRaft() {
	url, err := url.Parse(rn.peers[rn.id-1])
	if err != nil {
		log.Fatalf("dskv: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rn.httpstopc)
	if err != nil {
		log.Fatalf("dskv: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rn.transport.Handler()}).Serve(ln)
	select {
	case <-rn.httpstopc:
	default:
		log.Fatalf("dskv: Failed to serve rafthttp (%v)", err)
	}
	close(rn.httpdonec)
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

func (rn *raftNode) serveChannels() {
	snap, err := rn.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rn.confState = snap.Metadata.ConfState
	rn.snapshotIndex = snap.Metadata.Index
	rn.appliedIndex = snap.Metadata.Index
	defer rn.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		var confChangeCount uint64 = 0
		for rn.proposeC != nil && rn.confChangeC != nil {
			select {
			case prop, ok := <-rn.proposeC:
				if !ok {
					rn.proposeC = nil
				} else {
					rn.node.Propose(context.TODO(), []byte(prop))
				}
			case cc, ok := <-rn.confChangeC:
				if !ok {
					rn.confChangeC = nil
				} else {
					confChangeCount += 1
					cc.ID = confChangeCount
					rn.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		close(rn.stopc)
	}()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
		case rd := <-rn.node.Ready():
			rn.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.saveSnap(rd.Snapshot)
				rn.raftStorage.ApplySnapshot(rd.Snapshot)
				rn.publishSnapshot(rd.Snapshot)
			}
			rn.raftStorage.Append(rd.Entries)
			rn.transport.Send(rd.Messages)
			if ok := rn.publishEntries(rn.entriesToApply(rd.CommittedEntries)); !ok {
				rn.stop()
				return
			}
			rn.maybeTriggerSnapshot()
			rn.node.Advance()
		case err := <-rn.transport.ErrorC:
			rn.writeError(err)
			return
		case <-rn.stopc:
			rn.stop()
			return
		}
	}
}

func (rn *raftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rn.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rn.snapshotter.SaveSnap(snap); err != nil {
		return err
	}

	return rn.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rn *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}
	log.Printf("publishing snapshot at index %d", rn.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rn.snapshotIndex)
	if snapshotToSave.Metadata.Index <= rn.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, rn.appliedIndex)
	}
	rn.commitC <- nil
	rn.confState = snapshotToSave.Metadata.ConfState
	rn.snapshotIndex = snapshotToSave.Metadata.Index
	rn.appliedIndex = snapshotToSave.Metadata.Index
}

func (rn *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rn.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("dskv: error loading snapshot (%v)", err)
	}
	return snapshot
}

func (rn *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				break
			}
			s := string(ents[i].Data)
			select {
			case rn.commitC <- &s:
			case <-rn.stopc:
				return false
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rn.confState = *rn.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rn.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rn.id) {
					log.Println("I've been removed from cluster! shutting down.")
					return false
				}
				rn.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
		rn.appliedIndex = ents[i].Index
		if ents[i].Index == rn.lastIndex {
			select {
			case rn.commitC <- nil:
			case <-rn.stopc:
				return false
			}
		}
	}
	return true
}

func (rn *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rn.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rn.appliedIndex)
	}
	if rn.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rn.appliedIndex-firstIdx+1:]
	}
	return
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

func (rn *raftNode) stop() {
	rn.stopHTTP()
	close(rn.commitC)
	close(rn.errorC)
	rn.node.Stop()
}

func (rn *raftNode) stopHTTP() {
	rn.transport.Stop()
	close(rn.httpstopc)
	<-rn.httpdonec
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rn *raftNode) maybeTriggerSnapshot() {
	if rn.appliedIndex-rn.snapshotIndex <= rn.snapCount {
		return
	}
	log.Printf("start snapshot [applied index: %d | last snapshot index : %d]", rn.appliedIndex, rn.snapshotIndex)
	data, err := rn.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rn.raftStorage.CreateSnapshot(rn.appliedIndex, &rn.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rn.saveSnap(snap); err != nil {
		panic(err)
	}
	compactIndex := uint64(1)
	if rn.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rn.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rn.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}
	log.Printf("compated log at index %d", compactIndex)
	rn.snapshotIndex = rn.appliedIndex
}

func (rn *raftNode) writeError(err error) {
	rn.stopHTTP()
	close(rn.commitC)
	rn.errorC <- err
	close(rn.errorC)
	rn.node.Stop()
}

func (rn *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rn.node.Step(ctx, m)
}

func (rn *raftNode) IsIDRemoved(id uint64) bool {
	return false
}

func (rn *raftNode) ReportUnreachable(id uint64) {}

func (rn *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
