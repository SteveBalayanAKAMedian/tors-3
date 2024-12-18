package main

import (
	"crdt/internal/crdt"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/lmittmann/tint"
)

func main() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelInfo,
		}),
	))

	var (
		replicaID string
		port      int
		peers     []string
	)

	flag.StringVar(&replicaID, "id", "", "Id for replicaset")
	flag.IntVar(&port, "port", 0, "HTTP-port for replica")
	flag.Parse()
	peers = flag.Args()
	slog.Info("Got another replicas", "peers", peers)

	crdt := crdt.NewCRDT(replicaID, peers)

	http.HandleFunc("/patch", crdt.PatchHandler)
	http.HandleFunc("/sync", crdt.SyncHandler)
	http.HandleFunc("GET /get", crdt.GetHandler)

	slog.Info("Starting server...", "port", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
