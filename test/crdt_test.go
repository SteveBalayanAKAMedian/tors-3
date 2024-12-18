package crdt_test

import (
	testutils "crdt/test/test_utils"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func TestBasic(t *testing.T) {
	cluster := testutils.NewTestCluster(3)
	err := cluster[0].PatchServer(map[string]string{
		"1": "2",
	})
	assert.NoError(t, err)
	cluster[0].ForceSyncServer()
	time.Sleep(1 * time.Second)

	for _, server := range cluster {
		assert.Equal(t, 1, len(server.GetHistory()))
		assert.Equal(t, "2", server.GetValue("1"))
	}
}

func TestSyncAllNodes(t *testing.T) {
	cluster := testutils.NewTestCluster(3)

	for i, server := range cluster {
		err := server.PatchServer(map[string]string{
			fmt.Sprint(i): fmt.Sprint(i + 1),
		})
		assert.NoError(t, err)
	}

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	time.Sleep(1 * time.Second)

	for _, server := range cluster {
		assert.Equal(t, 3, len(server.GetHistory()))
		assert.Equal(t, map[string]string{
			"0": "1",
			"1": "2",
			"2": "3",
		}, server.GetData())
	}
}

func TestRecovery(t *testing.T) {
	cluster := testutils.NewTestCluster(3)
	cluster[2].StopServer()

	for i, server := range cluster {
		if i == 2 {
			continue
		}

		err := server.PatchServer(map[string]string{
			fmt.Sprint(i): fmt.Sprint(i + 1),
		})
		assert.NoError(t, err)
	}

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	cluster[2].StartTestServer()

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	time.Sleep(1 * time.Second)

	for _, server := range cluster {
		assert.Equal(t, 2, len(server.GetHistory()))
		assert.Equal(t, map[string]string{
			"0": "1",
			"1": "2",
		}, server.GetData())
	}
}

func TestConflict(t *testing.T) {
	cluster := testutils.NewTestCluster(3)

	err := cluster[0].PatchServer(map[string]string{
		"1": "2",
	})
	assert.NoError(t, err)

	err = cluster[1].PatchServer(map[string]string{
		"1": "3",
	})
	assert.NoError(t, err)

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	time.Sleep(1 * time.Second)

	for i, server := range cluster {
		if i == 0 {
			assert.Equal(t, 2, len(server.GetHistory()))
		} else if i == 1 {
			assert.Equal(t, 1, len(server.GetHistory()))
		}

		assert.Equal(t, map[string]string{
			"1": "3",
		}, server.GetData())
	}
}

func TestStrongEventualConsistency(t *testing.T) {
	cluster := testutils.NewTestCluster(3)

	rand.Seed(uint64(time.Now().UnixNano()))
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			replica := cluster[rand.Intn(len(cluster))]
			patch := randomPatch()

			numRetry := 0
			for {
				if err := replica.PatchServer(patch); err != nil {
					if numRetry == 5 {
						slog.Error("cannot patch server", "error", err)
						os.Exit(1)
					} else {
						numRetry++
						continue
					}
				} else {
					break
				}
			}
		}()
	}
	wg.Wait()

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	time.Sleep(3 * time.Second)

	for i := 1; i < len(cluster); i++ {
		assert.Equal(t, cluster[0].GetData(), cluster[i].GetData())
	}
}

func randomPatch() map[string]string {
	key := fmt.Sprintf("key%d", rand.Intn(5))
	value := fmt.Sprintf("value%d", rand.Intn(1000))
	return map[string]string{key: value}
}
