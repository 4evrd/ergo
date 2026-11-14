// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package irc

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ergochat/ergo/irc/cluster"
	"github.com/ergochat/ergo/irc/i18n"
)

type testNode struct {
	server *Server
	addr   string
}

// startClusterNodes starts in-process ergo servers that form one cluster
func startClusterNodes(t *testing.T, names ...string) []testNode {
	t.Helper()
	return startClusterNodesWith(t, nil, names...)
}

func startClusterNodesWith(t *testing.T, extraEdits map[string]string, names ...string) []testNode {
	t.Helper()
	preserveGlobals(t)
	routePorts := make([]int, len(names))
	for i := range routePorts {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		routePorts[i] = l.Addr().(*net.TCPAddr).Port
		l.Close()
	}
	var routes strings.Builder
	routes.WriteString("        routes:\n")
	for _, p := range routePorts {
		fmt.Fprintf(&routes, "            - \"nats-route://127.0.0.1:%d\"\n", p)
	}

	var nodes []testNode
	for i, name := range names {
		dir := t.TempDir()
		edits := merge(clusterSection(dir, name), map[string]string{
			"        node-timeout: 15s": "        node-timeout: 3s",
			// reconciliation is driven by the tests
			"        reconcile-interval: 30s": "        reconcile-interval: 1h",
		})
		for k, v := range extraEdits {
			if k == "        reconcile-interval: 1h" {
				edits["        reconcile-interval: 30s"] = v
			} else {
				edits[k] = v
			}
		}
		if len(names) > 1 {
			edits[`        #listen: "0.0.0.0:6222"`] = fmt.Sprintf(`        listen: "127.0.0.1:%d"`, routePorts[i])
			edits[`        #routes:
        #    - "nats-route://node1.example.com:6222"
        #    - "nats-route://node2.example.com:6222"
        #    - "nats-route://node3.example.com:6222"
`] = routes.String()
		}
		path, addr := writeTestConfigAddr(t, dir, edits, "")
		server := startTestServer(t, dir, path)
		t.Cleanup(func() {
			// test connections are closed by now; let their goroutines finish
			deadline := time.Now().Add(5 * time.Second)
			for len(server.clients.AllClients()) != 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			server.Shutdown()
		})
		nodes = append(nodes, testNode{server: server, addr: addr})
	}
	for _, node := range nodes {
		if !node.server.clusterNicks.reg().WaitReady(30 * time.Second) {
			t.Fatal("nick registry did not become ready")
		}
	}
	return nodes
}

type ircConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dialIRC(t *testing.T, addr string) *ircConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &ircConn{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *ircConn) send(line string) {
	c.t.Helper()
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write([]byte(line + "\r\n")); err != nil {
		c.t.Fatal(err)
	}
}

// expect reads lines until one has a command in commands, and returns it
func (c *ircConn) expect(commands ...string) (command string, line string) {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			c.t.Fatalf("waiting for %v: %v", commands, err)
		}
		line = strings.TrimRight(line, "\r\n")
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.HasPrefix(fields[0], "@") {
			fields = fields[1:]
		}
		if len(fields) > 0 && strings.HasPrefix(fields[0], ":") {
			fields = fields[1:]
		}
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "PING" {
			c.send("PONG " + strings.Join(fields[1:], " "))
			continue
		}
		for _, cmd := range commands {
			if fields[0] == cmd {
				return cmd, line
			}
		}
	}
}

// register connects a client with nick and returns the numeric that ended
// registration: 001 on success, or the error numeric
func register(t *testing.T, addr, nick string) (*ircConn, string) {
	t.Helper()
	c := dialIRC(t, addr)
	c.send("NICK " + nick)
	c.send("USER u 0 * :test user")
	cmd, _ := c.expect(RPL_WELCOME, ERR_NICKNAMEINUSE, ERR_UNAVAILRESOURCE, ERR_ERRONEUSNICKNAME)
	return c, cmd
}

func eventuallyTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestClusterNickUniqueness(t *testing.T) {
	nodes := startClusterNodes(t, "node1", "node2")
	a, b := nodes[0], nodes[1]

	alice, numeric := register(t, a.addr, "alice")
	assertNumeric(t, numeric, RPL_WELCOME)

	// the nick and its case/confusable variants are taken network-wide
	_, numeric = register(t, b.addr, "alice")
	assertNumeric(t, numeric, ERR_NICKNAMEINUSE)
	_, numeric = register(t, b.addr, "ALICE")
	assertNumeric(t, numeric, ERR_NICKNAMEINUSE)

	// changing case on the owning node is fine
	alice.send("NICK Alice")
	alice.expect("NICK")

	// a nick change releases the old nick and claims the new one
	alice.send("NICK carol")
	alice.expect("NICK")
	bob, numeric := register(t, b.addr, "bob")
	assertNumeric(t, numeric, RPL_WELCOME)
	bob.send("NICK carol")
	bob.expect(ERR_NICKNAMEINUSE)
	bob.send("NICK alice")
	bob.expect("NICK")

	// bob's old nick was released by the change...
	_, numeric = register(t, a.addr, "bob")
	assertNumeric(t, numeric, RPL_WELCOME)

	// ...and quitting releases the current one
	_, numeric = register(t, a.addr, "alice")
	assertNumeric(t, numeric, ERR_NICKNAMEINUSE)
	bob.send("QUIT :bye")
	eventuallyTrue(t, "alice to be released", func() bool {
		_, found, _ := a.server.clusterNicks.reg().Lookup("alice")
		return !found
	})
	_, numeric = register(t, a.addr, "alice")
	assertNumeric(t, numeric, RPL_WELCOME)
}

// confusables are only folded together under precis casemapping
// (go test -tags i18n)
func TestClusterNickConfusables(t *testing.T) {
	if !i18n.Enabled {
		t.Skip("i18n support not compiled in")
	}
	nodes := startClusterNodesWith(t, map[string]string{`    casemapping: "ascii"`: `    casemapping: "precis"`}, "node1", "node2")
	a, b := nodes[0], nodes[1]
	_, numeric := register(t, a.addr, "alice")
	assertNumeric(t, numeric, RPL_WELCOME)
	_, numeric = register(t, b.addr, "ALICE")
	assertNumeric(t, numeric, ERR_NICKNAMEINUSE)
	_, numeric = register(t, b.addr, "\u0430lice") // Cyrillic а: same skeleton as alice
	assertNumeric(t, numeric, ERR_NICKNAMEINUSE)
	_, numeric = register(t, b.addr, "bob")
	assertNumeric(t, numeric, RPL_WELCOME)
}

func assertNumeric(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("got numeric %s, want %s", got, want)
	}
}

func TestClusterNickReconcile(t *testing.T) {
	nodes := startClusterNodes(t, "node1", "node2")
	a, b := nodes[0], nodes[1]
	ra, rb := a.server.clusterNicks.reg(), b.server.clusterNicks.reg()

	// a stale claim with no local client (e.g. left by a crash) is released
	if _, err := ra.Claim("ghost", cluster.NickClaim{Nick: "ghost"}); err != nil {
		t.Fatal(err)
	}
	released, _, _ := a.server.clusterNicks.reconcile()
	assertCount(t, "released", released, 1)
	if _, found, _ := ra.Lookup("ghost"); found {
		t.Error("stale claim survived reconciliation")
	}

	// a local client whose claim went missing gets it back
	_, numeric := register(t, a.addr, "dave")
	assertNumeric(t, numeric, RPL_WELCOME)
	if err := ra.Release("dave"); err != nil {
		t.Fatal(err)
	}
	_, claimed, _ := a.server.clusterNicks.reconcile()
	assertCount(t, "claimed", claimed, 1)
	if claim, found, _ := rb.Lookup("dave"); !found || claim.Node != "node1" {
		t.Errorf("claim not restored: %+v %v", claim, found)
	}

	// a nick another node took while this one was cut off: the local client
	// is renamed
	erin, numeric := register(t, a.addr, "erin")
	assertNumeric(t, numeric, RPL_WELCOME)
	ra.Release("erin") // simulate the claim being lost...
	if _, err := rb.Claim("erin", cluster.NickClaim{Nick: "erin"}); err != nil {
		t.Fatal(err) // ...and taken by node2
	}
	_, _, renamed := a.server.clusterNicks.reconcile()
	assertCount(t, "renamed", renamed, 1)
	_, line := erin.expect("NICK")
	if !strings.HasPrefix(strings.TrimPrefix(strings.Fields(line)[0], ":"), "erin!") {
		t.Errorf("unexpected NICK line %q", line)
	}
	if a.server.clients.Get("erin") != nil {
		t.Error("erin should have been renamed")
	}

	// nothing left to fix
	released, claimed, renamed = a.server.clusterNicks.reconcile()
	assertCount(t, "changes", released+claimed+renamed, 0)
}

func assertCount(t *testing.T, what string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %d, want %d", what, got, want)
	}
}

func TestClusterNickRegistryUnavailable(t *testing.T) {
	nodes := startClusterNodes(t, "solo")
	node := nodes[0]
	cn := node.server.clusterNicks
	working := cn.reg()

	_, numeric := register(t, node.addr, "frank")
	assertNumeric(t, numeric, RPL_WELCOME)
	frank := node.server.clients.Get("frank")

	// with the registry down, new nicks are refused rather than risk duplicates
	cn.registry.Store(&cluster.Nicks{})
	_, numeric = register(t, node.addr, "grace")
	assertNumeric(t, numeric, ERR_UNAVAILRESOURCE)

	// ...including NICK from registered clients (which carry no session)
	if _, err, _ := node.server.clients.SetNick(frank, nil, "frankie", false); err != errNickUnavailable {
		t.Fatalf("expected errNickUnavailable for a registered client, got %v", err)
	}
	// only restoring a session-less always-on client is let through
	acquired, err := cn.claim("restored", "restored", "restored", true)
	assertCount(t, "acquired", boolInt(acquired), 0)
	if err != nil {
		t.Fatalf("lenient claim failed: %v", err)
	}
	// reconciliation can't run, and does nothing
	released, claimed, renamed := cn.reconcile()
	assertCount(t, "changes", released+claimed+renamed, 0)

	cn.registry.Store(working)
	released, claimed, renamed = cn.reconcile()
	assertCount(t, "changes", released+claimed+renamed, 0)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestClusterNickStripes(t *testing.T) {
	cn := &clusterNicks{}
	// locking the same stripe twice in one call must not deadlock
	unlock := cn.lock("same", "same", "*", "")
	unlock()
	if validSkeleton("*") || validSkeleton("") || !validSkeleton("x") {
		t.Error("validSkeleton")
	}
	cn.release("*") // no-op for placeholder skeletons
}

func TestClusterNicksDisabled(t *testing.T) {
	preserveGlobals(t)
	dir := t.TempDir()
	edits := clusterSection(dir, "node1")
	edits["        enabled: true\n        # a node that hasn't sent a heartbeat"] = "        enabled: false\n        # a node that hasn't sent a heartbeat"
	path, _ := writeTestConfigAddr(t, dir, edits, "")
	server := startTestServer(t, dir, path)
	defer server.Shutdown()
	if server.cluster == nil || server.clusterNicks != nil {
		t.Error("expected clustering without nick enforcement")
	}
}

func TestClusterNickReleasedWhenReservedLocally(t *testing.T) {
	nodes := startClusterNodes(t, "solo")
	node := nodes[0]
	reg := node.server.clusterNicks.reg()

	// register an account, so its nick becomes reserved
	zed, numeric := register(t, node.addr, "zed")
	assertNumeric(t, numeric, RPL_WELCOME)
	zed.send("PRIVMSG NickServ :REGISTER hunter2")
	zed.expect("NOTICE")
	zed.send("QUIT")
	eventuallyTrue(t, "zed to disconnect", func() bool { return node.server.clients.Get("zed") == nil })

	// claiming it cluster-wide succeeds, but it's refused locally (reserved),
	// so the claim must be given back
	_, numeric = register(t, node.addr, "zed")
	assertNumeric(t, numeric, ERR_NICKNAMEINUSE)
	if _, found, _ := reg.Lookup("zed"); found {
		t.Error("claim on a locally refused nick was not released")
	}
}

func TestClusterSanickUnavailable(t *testing.T) {
	nodes := startClusterNodes(t, "solo")
	node := nodes[0]
	_, numeric := register(t, node.addr, "oper")
	assertNumeric(t, numeric, RPL_WELCOME)
	_, numeric = register(t, node.addr, "victim")
	assertNumeric(t, numeric, RPL_WELCOME)
	oper, victim := node.server.clients.Get("oper"), node.server.clients.Get("victim")

	cn := node.server.clusterNicks
	working := cn.reg()
	cn.registry.Store(&cluster.Nicks{})
	defer cn.registry.Store(working)
	rb := NewResponseBuffer(oper.Sessions()[0])
	err := performNickChange(node.server, oper, victim, nil, "renamed", rb)
	if err != errNickUnavailable {
		t.Errorf("expected errNickUnavailable, got %v", err)
	}
}

func TestClusterNickPeriodicReconcile(t *testing.T) {
	nodes := startClusterNodesWith(t, map[string]string{"        reconcile-interval: 1h": "        reconcile-interval: 1s"}, "solo")
	reg := nodes[0].server.clusterNicks.reg()
	if _, err := reg.Claim("ghost", cluster.NickClaim{Nick: "ghost"}); err != nil {
		t.Fatal(err)
	}
	eventuallyTrue(t, "background reconciliation to release the stale claim", func() bool {
		_, found, _ := reg.Lookup("ghost")
		return !found
	})
}

func TestClusterNickAlwaysOnRestore(t *testing.T) {
	preserveGlobals(t)
	dir := t.TempDir()
	edits := clusterSection(dir, "solo")

	path, addr := writeTestConfigAddr(t, dir, edits, "")
	server := startTestServer(t, dir, path)
	reg := server.clusterNicks.reg()
	reg.WaitReady(10 * time.Second)

	// an always-on account client
	amy, numeric := register(t, addr, "amy")
	assertNumeric(t, numeric, RPL_WELCOME)
	amy.send("PRIVMSG NickServ :REGISTER hunter2")
	amy.expect("NOTICE")
	amy.send("PRIVMSG NickServ :SET always-on true")
	amy.expect("NOTICE")
	amy.send("QUIT")
	time.Sleep(100 * time.Millisecond)
	if server.clients.Get("amy") == nil {
		t.Fatal("always-on client did not persist")
	}
	// simulate the claim being lost, and leave a stale claim behind
	reg.Release("amy")
	reg.Claim("ghost", cluster.NickClaim{Nick: "ghost"})
	server.Shutdown()

	// restart on the same data: the restored client claims its nick, and the
	// stale claim from the previous run is released
	path, _ = writeTestConfigAddr(t, dir, edits, "")
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	lm := quietTestLogger()
	server, err = NewServer(config, lm)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown()
	if server.clients.Get("amy") == nil {
		t.Fatal("always-on client was not restored")
	}
	reg = server.clusterNicks.reg()
	if claim, found, _ := reg.Lookup("amy"); !found || claim.Node != "solo" || claim.Account != "amy" {
		t.Errorf("restored client's nick not claimed: %+v %v", claim, found)
	}
	if _, found, _ := reg.Lookup("ghost"); found {
		t.Error("stale claim from the previous run was not released")
	}
}
