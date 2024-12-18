package testutils

import (
	"bytes"
	"context"
	"crdt/internal/crdt"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/lmittmann/tint"
)

const (
	beginCrdtPort = 8080
)

type TestCRDTServer struct {
	httpServeMux *http.ServeMux
	httpServer   *http.Server

	crdtServer *crdt.CRDT
	address    string
}

func (t *TestCRDTServer) StartTestServer() {
	t.httpServer = &http.Server{
		Addr:    t.address,
		Handler: t.httpServeMux,
	}
	go func() {
		slog.Info("Starting server...", "address", t.address)
		if err := t.httpServer.ListenAndServe(); err != nil {
			slog.Error("error listening server", "error", err)
		}
	}()
}

func (t *TestCRDTServer) StopServer() {
	t.httpServer.Shutdown(context.TODO())
}

func (t *TestCRDTServer) PatchServer(patchValues map[string]string) error {
	jsonString, err := json.Marshal(patchValues)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost%s/patch", t.address), bytes.NewBuffer(jsonString))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func (t *TestCRDTServer) ForceSyncServer() {
	t.crdtServer.ForceHeartbeat()
}

func (t *TestCRDTServer) GetHistory() []crdt.Operation {
	return t.crdtServer.GetHistory()
}

func (t *TestCRDTServer) GetTimestamps() map[string]map[string]int {
	return t.crdtServer.GetTimestamps()
}

func (t *TestCRDTServer) GetValue(key string) string {
	return t.crdtServer.GetValue(key)
}

func (t *TestCRDTServer) GetData() map[string]string {
	return t.crdtServer.GetData()
}

func NewTestCluster(count int) []*TestCRDTServer {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelInfo,
		}),
	))

	result := make([]*TestCRDTServer, 0, count)

	peers := []string{}
	for id := 0; id < count; id++ {
		peers = append(peers, fmt.Sprintf(":%d", beginCrdtPort+id))
	}

	for id, peer := range peers {
		newServer := &TestCRDTServer{
			crdtServer: crdt.NewCRDT(
				peer,
				Filter(peers, func(s string) bool {
					return s != fmt.Sprintf(":%d", beginCrdtPort+id)
				}),
			),
			address:      peer,
			httpServeMux: http.NewServeMux(),
		}
		newServer.httpServeMux.HandleFunc("/patch", newServer.crdtServer.PatchHandler)
		newServer.httpServeMux.HandleFunc("/sync", newServer.crdtServer.SyncHandler)

		newServer.StartTestServer()
		result = append(result, newServer)
	}

	return result
}

func Filter(ss []string, test func(string) bool) (ret []string) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}
