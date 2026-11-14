// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package irc

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/sqlite"
	"github.com/ergochat/ergo/irc/utils"
)

// writeTestConfig derives a runnable config from default.yaml: a random
// loopback listener, no TLS, data under dir, and the given edits applied
func writeTestConfig(t *testing.T, dir string, edits map[string]string, extra string) string {
	t.Helper()
	raw, err := os.ReadFile("../default.yaml")
	if err != nil {
		t.Fatal(err)
	}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	l.Close()

	conf := string(raw)
	replace := func(old, new string) {
		t.Helper()
		if !strings.Contains(conf, old) {
			t.Fatalf("default.yaml no longer contains %q", old)
		}
		conf = strings.Replace(conf, old, new, 1)
	}
	replace(`        "127.0.0.1:6667": # (loopback ipv4, localhost-only)
        "[::1]:6667":     # (loopback ipv6, localhost-only)`, fmt.Sprintf(`        "%s":`, addr))
	replace(`        ":6697":
            # this is a standard TLS configuration with a single certificate;
            # see the manual for instructions on how to configure SNI
            tls:
                cert: fullchain.pem
                key: privkey.pem`, `        "127.0.0.1:0":`)
	replace("    path: ircd.db", "    path: "+filepath.Join(dir, "ircd.db"))
	replace(`lock-file: "ircd.lock"`, fmt.Sprintf("lock-file: %q", filepath.Join(dir, fmt.Sprintf("ircd-%d.lock", time.Now().UnixNano()))))
	replace(`languages:
    # whether to load languages
    enabled: true`, `languages:
    # whether to load languages
    enabled: false`)
	for old, new := range edits {
		replace(old, new)
	}
	conf += "\n" + extra

	path := filepath.Join(dir, fmt.Sprintf("ircd-%d.yaml", time.Now().UnixNano()))
	if err := os.WriteFile(path, []byte(conf), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func pebbleEdits(dir string) map[string]string {
	return map[string]string{
		`    pebble:
        enabled: false
        # directory holding the Pebble database
        path: "ergo_history.pebble"`: `    pebble:
        enabled: true
        # directory holding the Pebble database
        path: "` + filepath.Join(dir, "history.pebble") + `"`,
		`    persistent:
        enabled: false

        # store unregistered channel messages in the persistent database?
        unregistered-channels: false`: `    persistent:
        enabled: true

        # store unregistered channel messages in the persistent database?
        unregistered-channels: true`,
	}
}

func clusterSection(dir, node string) string {
	return fmt.Sprintf(`cluster:
    enabled: true
    node-name: %s
    embedded:
        store-dir: %q
    history:
        enabled: true
`, node, filepath.Join(dir, "nats"))
}

func TestClusterConfigValidation(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]struct {
		edits map[string]string
		extra string
		want  string
	}{
		"history replication without cluster": {
			extra: "cluster:\n    history:\n        enabled: true\n",
			want:  "cluster.history.enabled requires cluster.enabled",
		},
		"invalid node name": {
			extra: "cluster:\n    enabled: true\n    node-name: \"bad name\"\n",
			want:  "cluster.node-name",
		},
		"persistent history without a backend": {
			edits: map[string]string{
				"    persistent:\n        enabled: false": "    persistent:\n        enabled: true",
			},
			want: "Pebble",
		},
	}
	for name, tc := range cases {
		_, err := LoadConfig(writeTestConfig(t, dir, tc.edits, tc.extra))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got error %v, want one containing %q", name, err, tc.want)
		}
	}

	// a valid pebble + cluster config loads, with derived settings filled in
	config, err := LoadConfig(writeTestConfig(t, dir, pebbleEdits(dir), clusterSection(dir, "node1")))
	if err != nil {
		t.Fatal(err)
	}
	if !config.Datastore.Pebble.Enabled || !config.Cluster.Enabled || config.Cluster.Replicas != 1 {
		t.Errorf("unexpected config: %+v %+v", config.Datastore.Pebble, config.Cluster)
	}
	if config.Datastore.Pebble.ExpireTime != config.Cluster.HistoryExpireTime {
		t.Error("history expire time not propagated")
	}
}

// applyConfig sets package-level state on the initial load; restore it so
// other tests in the package aren't affected
func preserveGlobals(t *testing.T) {
	casemapping, utf8, maxLine := globalCasemappingSetting, globalUtf8EnforcementSetting, MaxLineLen
	register, ping, disconnect := RegisterTimeout, PingTimeout, DisconnectTimeout
	t.Cleanup(func() {
		globalCasemappingSetting, globalUtf8EnforcementSetting, MaxLineLen = casemapping, utf8, maxLine
		RegisterTimeout, PingTimeout, DisconnectTimeout = register, ping, disconnect
	})
}

func startTestServer(t *testing.T, dir string, configPath string) *Server {
	t.Helper()
	preserveGlobals(t)
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitDB(config.Datastore.Path); err != nil {
		t.Fatal(err)
	}
	lm, _ := logger.NewManager(nil)
	server, err := NewServer(config, lm)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestServerWithPebbleAndCluster(t *testing.T) {
	dir := t.TempDir()
	server := startTestServer(t, dir, writeTestConfig(t, dir, pebbleEdits(dir), clusterSection(dir, "node1")))

	if server.pebbleHistoryDB == nil || server.cluster == nil {
		t.Fatal("expected pebble history and a cluster connection")
	}
	if server.cluster.NodeName() != "node1" {
		t.Errorf("unexpected node name %s", server.cluster.NodeName())
	}
	// history is replicated through the cluster wrapper
	if fmt.Sprintf("%T", server.historyDB) != "*cluster.ReplicatedHistory" {
		t.Errorf("history database is %T, expected the replicating wrapper", server.historyDB)
	}
	item := history.Item{Type: history.Privmsg, Message: utils.MakeMessage("hello")}
	if err := server.historyDB.AddChannelItem("#test", item, ""); err != nil {
		t.Fatal(err)
	}

	// rehashing with a changed pebble config is applied; a changed cluster
	// config is refused
	edits := pebbleEdits(dir)
	edits["        cache-size-mb: 64"] = "        cache-size-mb: 32"
	newConfig, err := LoadConfig(writeTestConfig(t, dir, edits, clusterSection(dir, "node1")))
	if err != nil {
		t.Fatal(err)
	}
	newConfig.Server.Name = server.name
	if err := server.applyConfig(newConfig); err != nil {
		t.Fatalf("rehash failed: %v", err)
	}
	renamed, err := LoadConfig(writeTestConfig(t, dir, pebbleEdits(dir), clusterSection(dir, "node2")))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.applyConfig(renamed); err == nil || !strings.Contains(err.Error(), "Cluster configuration") {
		t.Errorf("expected rehash to refuse a cluster change, got %v", err)
	}

	server.Shutdown()
}

func TestServerWithPebbleOnly(t *testing.T) {
	dir := t.TempDir()
	server := startTestServer(t, dir, writeTestConfig(t, dir, pebbleEdits(dir), ""))
	if server.cluster != nil {
		t.Error("cluster should be disabled")
	}
	if server.historyDB != server.pebbleHistoryDB {
		t.Error("expected history to go directly to pebble")
	}
	server.Shutdown()
}

func TestServerStartupFailures(t *testing.T) {
	preserveGlobals(t)
	dir := t.TempDir()
	lm, _ := logger.NewManager(nil)

	// pebble can't open a regular file as its database directory
	bogus := filepath.Join(dir, "not-a-dir")
	os.WriteFile(bogus, []byte("x"), 0600)
	config, err := LoadConfig(writeTestConfig(t, dir, pebbleEditsWithPath(dir, bogus), ""))
	if err != nil {
		t.Fatal(err)
	}
	InitDB(config.Datastore.Path)
	if _, err := NewServer(config, lm); err == nil {
		t.Error("expected pebble open failure")
	}

	// the cluster can't start when its NATS server is unreachable
	dir2 := t.TempDir()
	extra := "cluster:\n    enabled: true\n    node-name: node1\n    url: \"nats://127.0.0.1:1\"\n"
	config, err = LoadConfig(writeTestConfig(t, dir2, pebbleEdits(dir2), extra))
	if err != nil {
		t.Fatal(err)
	}
	InitDB(config.Datastore.Path)
	if _, err := NewServer(config, lm); err == nil {
		t.Error("expected cluster startup failure")
	}
}

func pebbleEditsWithPath(dir, path string) map[string]string {
	edits := pebbleEdits(dir)
	for k, v := range edits {
		if strings.Contains(v, "history.pebble") {
			edits[k] = strings.Replace(v, filepath.Join(dir, "history.pebble"), path, 1)
		}
	}
	return edits
}

// only reachable when another persistent backend is compiled in
// (go test -tags sqlite)
func TestClusterHistoryRequiresPebble(t *testing.T) {
	if !sqlite.Enabled {
		t.Skip("SQLite support not compiled in")
	}
	dir := t.TempDir()
	edits := map[string]string{
		"    persistent:\n        enabled: false": "    persistent:\n        enabled: true",
		"    sqlite:\n        enabled: false":     "    sqlite:\n        enabled: true",
	}
	_, err := LoadConfig(writeTestConfig(t, dir, edits, clusterSection(dir, "node1")))
	if err == nil || !strings.Contains(err.Error(), "requires the Pebble history backend") {
		t.Errorf("expected the Pebble requirement error, got %v", err)
	}
}
