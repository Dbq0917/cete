package server

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/protobuf"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type testClusterNode struct {
	raftServer *RaftServer
	grpcServer *grpc.Server
	service    *GRPCService
	dataDir    string
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitFor(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestMajorityWriteAndConsistentRead(t *testing.T) {
	logger := zap.NewNop()
	ids := []string{"node1", "node2", "node3"}
	raftAddresses := []string{freeTCPAddress(t), freeTCPAddress(t), freeTCPAddress(t)}
	grpcAddresses := []string{freeTCPAddress(t), freeTCPAddress(t), freeTCPAddress(t)}

	nodes := make([]*testClusterNode, 3)
	for i := range nodes {
		dataDir, err := ioutil.TempDir("", "cete-quorum-")
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &testClusterNode{dataDir: dataDir}

		raftServer, err := NewRaftServer(ids[i], raftAddresses[i], dataDir, i == 0, logger)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].raftServer = raftServer
		if err := raftServer.Start(); err != nil {
			t.Fatal(err)
		}

		service, err := NewGRPCService(raftServer, "", "", logger)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].service = service

		listener, err := net.Listen("tcp", grpcAddresses[i])
		if err != nil {
			t.Fatal(err)
		}
		grpcServer := grpc.NewServer()
		protobuf.RegisterKVSServer(grpcServer, service)
		nodes[i].grpcServer = grpcServer
		go func() {
			_ = grpcServer.Serve(listener)
		}()
		if err := service.Start(); err != nil {
			t.Fatal(err)
		}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			if node == nil {
				continue
			}
			if node.service != nil {
				_ = node.service.Stop()
			}
			if node.grpcServer != nil {
				node.grpcServer.Stop()
			}
			if node.raftServer != nil {
				_ = node.raftServer.Stop()
			}
			_ = os.RemoveAll(node.dataDir)
		}
	})

	waitFor(t, 10*time.Second, "initial leader", func() bool {
		return nodes[0].raftServer.State() == raft.Leader
	})

	if err := nodes[0].raftServer.join(ids[0], &protobuf.Metadata{GrpcAddress: grpcAddresses[0]}); err != nil {
		t.Fatal(err)
	}

	for i := 1; i < len(nodes); i++ {
		err := nodes[0].raftServer.Join(ids[i], &protobuf.Node{
			RaftAddress: raftAddresses[i],
			Metadata: &protobuf.Metadata{
				GrpcAddress: grpcAddresses[i],
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, 10*time.Second, "three voters", func() bool {
		future := nodes[0].raftServer.raft.GetConfiguration()
		if future.Error() != nil {
			return false
		}
		if len(future.Configuration().Servers) != 3 {
			return false
		}
		for _, server := range future.Configuration().Servers {
			if server.Suffrage != raft.Voter {
				return false
			}
		}
		return true
	})

	waitFor(t, 10*time.Second, "node metadata", func() bool {
		for _, node := range nodes {
			for _, id := range ids {
				if node.raftServer.fsm.getMetadata(id) == nil {
					return false
				}
			}
		}
		return true
	})

	ctx := context.Background()
	setReq := &protobuf.SetRequest{Key: "key", Value: []byte("latest")}
	if _, err := nodes[1].service.Set(ctx, setReq); err != nil {
		t.Fatal(err)
	}

	if err := nodes[2].raftServer.fsm.kvs.Set("key", []byte("stale")); err != nil {
		t.Fatal(err)
	}

	getResp, err := nodes[2].service.Get(ctx, &protobuf.GetRequest{Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if string(getResp.Value) != "latest" {
		t.Fatalf("expected consistent read through leader, got %q", string(getResp.Value))
	}

	scanResp, err := nodes[2].service.Scan(ctx, &protobuf.ScanRequest{Prefix: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if len(scanResp.Values) != 1 || string(scanResp.Values[0]) != "latest" {
		t.Fatalf("expected consistent scan through leader, got %q", scanResp.Values)
	}

	var followerNode *testClusterNode
	for _, node := range nodes {
		if node.raftServer.State() != raft.Leader {
			followerNode = node
			break
		}
	}
	if followerNode == nil {
		t.Fatal("no follower to stop")
	}

	followerNode.grpcServer.Stop()
	_ = followerNode.service.Stop()
	followerNode.service = nil
	_ = followerNode.raftServer.Stop()
	followerNode.raftServer = nil

	start := time.Now()
	if _, err := nodes[0].service.Set(ctx, &protobuf.SetRequest{Key: "majority", Value: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("majority write took too long: %s", elapsed)
	}
}
