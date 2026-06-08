// Copyright (C) 2026  Przemysław Malkowski
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"html/template"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const version = "0.3.5"

var debugMode bool
var viewOnly  bool

// ── connection pool ───────────────────────────────────────────────────────────

var (
	clientsMu sync.RWMutex
	clients   = map[string]*mongo.Client{}
)

// parseCredentials extracts username and password from a MongoDB URI
func parseCredentials(uri string) (username, password string) {
	// Format: mongodb://username:password@host or mongodb://host
	if idx := strings.Index(uri, "://"); idx >= 0 {
		rest := uri[idx+3:]
		if atIdx := strings.Index(rest, "@"); atIdx >= 0 {
			userPass := rest[:atIdx]
			if colonIdx := strings.Index(userPass, ":"); colonIdx >= 0 {
				return userPass[:colonIdx], userPass[colonIdx+1:]
			}
		}
	}
	return "", ""
}

// injectCredentials adds username:password to a host URI
func injectCredentials(hostURI, username, password string) string {
	if username == "" || password == "" {
		return hostURI
	}
	// Extract just the host part
	schemeIdx := strings.Index(hostURI, "://")
	if schemeIdx < 0 {
		return hostURI
	}
	scheme := hostURI[:schemeIdx+3]
	hostPart := hostURI[schemeIdx+3:]
	
	// Remove any existing credentials
	if atIdx := strings.Index(hostPart, "@"); atIdx >= 0 {
		hostPart = hostPart[atIdx+1:]
	}
	
	return scheme + username + ":" + password + "@" + hostPart
}

// debugLog prints a message only if debug mode is enabled
func debugLog(format string, v ...interface{}) {
	if debugMode {
		msg := fmt.Sprintf(format, v...)
		log.Println("[DEBUG]", msg)
	}
}

func getClient(uri string) (*mongo.Client, error) {
	clientsMu.RLock()
	c, ok := clients[uri]
	clientsMu.RUnlock()
	if ok {
		return c, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	opts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Second).
		SetConnectTimeout(5 * time.Second)
	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err = c.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}
	clientsMu.Lock()
	clients[uri] = c
	clientsMu.Unlock()
	return c, nil
}

// ── BSON → JSON-safe ─────────────────────────────────────────────────────────

func bsonToAny(v interface{}) interface{} {
	switch x := v.(type) {
	case primitive.D:
		m := make(map[string]interface{}, len(x))
		for _, e := range x {
			m[e.Key] = bsonToAny(e.Value)
		}
		return m
	case primitive.A:
		a := make([]interface{}, len(x))
		for i, e := range x {
			a[i] = bsonToAny(e)
		}
		return a
	case primitive.ObjectID:
		return map[string]string{"$oid": x.Hex()}
	case primitive.DateTime:
		return x.Time().UTC().Format(time.RFC3339)
	case primitive.Timestamp:
		return map[string]interface{}{"$timestamp": map[string]uint32{"t": x.T, "i": x.I}}
	case primitive.Binary:
		return map[string]interface{}{"$binary": fmt.Sprintf("%x", x.Data), "subType": x.Subtype}
	case primitive.Decimal128:
		return x.String()
	case primitive.Regex:
		return map[string]string{"$regex": x.Pattern, "$options": x.Options}
	case primitive.JavaScript:
		return map[string]string{"$code": string(x)}
	case primitive.MinKey:
		return map[string]int{"$minKey": 1}
	case primitive.MaxKey:
		return map[string]int{"$maxKey": 1}
	case primitive.Undefined, nil:
		return nil
	case primitive.Symbol:
		return string(x)
	case int32:
		return x
	case int64:
		return x
	case float64:
		// JSON cannot represent IEEE 754 NaN or ±Inf (mongo returns NaN for
		// avgObjSize on empty collections, etc.). Map them to nil so the
		// encoder never hits an UnsupportedValueError mid-stream.
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil
		}
		return x
	case bool:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

func runCmd(uri, dbName string, cmd bson.D) (map[string]interface{}, error) {
	c, err := getClient(uri)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var raw bson.D
	if err := c.Database(dbName).RunCommand(ctx, cmd).Decode(&raw); err != nil {
		return nil, err
	}
	return bsonToAny(raw).(map[string]interface{}), nil
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func jsonErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func jsonErrWithShard(w http.ResponseWriter, msg string, shardID string, originalURI string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
		"failedShard": shardID,
		"failedUri": originalURI,
	})
}

func jsonOK(w http.ResponseWriter, v interface{}) {
	// Marshal to a buffer first. If the value contains an un-encodable type
	// (e.g. any NaN that slipped through bsonToAny) we return a clean error
	// JSON instead of writing truncated bytes to the response stream.
	b, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		_, _ = fmt.Fprintf(w, `{"error":"json encoding failed: %s"}`, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
	_, _ = w.Write([]byte{'\n'})
}

func formURI(r *http.Request) string { return strings.TrimSpace(r.FormValue("uri")) }

// parseShardCredentials extracts per-shard credentials from form data
func parseShardCredentials(r *http.Request) map[string]struct{ username, password string } {
	creds := make(map[string]struct{ username, password string })
	ids := r.Form["shard_creds_id[]"]
	users := r.Form["shard_creds_user[]"]
	passes := r.Form["shard_creds_pass[]"]
	
	for i := 0; i < len(ids) && i < len(users) && i < len(passes); i++ {
		creds[ids[i]] = struct{ username, password string }{users[i], passes[i]}
	}
	return creds
}

// applyShardCredentials injects shard-specific credentials into a URI if available
func applyShardCredentials(uri string, shardCreds map[string]struct{ username, password string }) string {
	if len(shardCreds) > 0 {
		debugLog(fmt.Sprintf("applyShardCredentials: checking %d credential sets for URI: %s", len(shardCreds), maskURI(uri)))
	}
	
	// Try to match shard ID in the URI
	for shardID, creds := range shardCreds {
		if creds.username == "" || creds.password == "" {
			continue
		}
		
		debugLog(fmt.Sprintf("applyShardCredentials: trying to match shardID '%s' (user: %s)", shardID, creds.username))
		
		// Check if shardID is contained in the URI
		if strings.Contains(uri, shardID) {
			debugLog(fmt.Sprintf("applyShardCredentials: MATCHED (direct contains) - injecting credentials for %s", shardID))
			return injectCredentials(uri, creds.username, creds.password)
		}
		
		// Also check if the URI contains this identifier after the @
		// (in case we're matching hostname:port style identifiers)
		if idx := strings.Index(uri, "@"); idx >= 0 {
			hostPart := uri[idx+1:]
			if strings.Contains(hostPart, shardID) {
				debugLog(fmt.Sprintf("applyShardCredentials: MATCHED (hostname contains) - injecting credentials for %s", shardID))
				return injectCredentials(uri, creds.username, creds.password)
			}
		}
		
		debugLog(fmt.Sprintf("applyShardCredentials: NO MATCH for %s", shardID))
	}
	return uri
}

// Helper to mask URI for logging
func maskURI(uri string) string {
	// Find credentials section: mongodb://username:password@host
	atIdx := strings.Index(uri, "@")
	if atIdx <= 0 {
		return uri // no credentials, nothing to mask
	}
	schemeEnd := strings.Index(uri, "://")
	if schemeEnd < 0 {
		return uri
	}
	scheme := uri[:schemeEnd+3] // "mongodb://"
	credentials := uri[schemeEnd+3 : atIdx] // "username:password"
	rest := uri[atIdx:] // "@host:port/..."

	colonIdx := strings.Index(credentials, ":")
	if colonIdx < 0 {
		// No password, just username
		return scheme + credentials + rest
	}
	username := credentials[:colonIdx]
	return scheme + username + ":***@" + uri[atIdx+1:]
}

// extractShardID tries to extract shard ID from a URI
func extractShardID(uri string) string {
	// Look for shard00, shard01, etc. in the URI
	re := regexp.MustCompile(`(shard\d+)`)
	matches := re.FindStringSubmatch(uri)
	if len(matches) > 1 {
		return matches[1]
	}
	// Fallback to extracting hostname
	if idx := strings.Index(uri, "://"); idx >= 0 {
		rest := uri[idx+3:]
		if atIdx := strings.Index(rest, "@"); atIdx >= 0 {
			rest = rest[atIdx+1:]
		}
		if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
			rest = rest[:slashIdx]
		}
		return rest
	}
	return uri
}

// hostFromURI returns the bare host:port (or RS host list) from a connection
// URI, stripping scheme, credentials and query string — for display only.
func hostFromURI(uri string) string {
	s := uri
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[i+1:]
	}
	if i := strings.IndexAny(s, "/?"); i >= 0 {
		s = s[:i]
	}
	return s
}

// ── topology (continued) ───────────────────────────────────────────────────────

// NodeInfo describes one addressable group in the cluster.
type NodeInfo struct {
	ID   string `json:"id"`   // human label: "shard01", "configsvr", "mongos-0"
	Role string `json:"role"` // "shard" | "configsvr" | "mongos"
	URI  string `json:"uri"`  // connection URI
}

// extractPort returns the port number from a "host:port" string.
// Returns "27017" as default if no port is found.
func extractPort(hostport string) string {
	// Strip any leading mongodb:// and credentials
	s := hostport
	if idx := strings.Index(s, "://"); idx >= 0 {
		s = s[idx+3:]
	}
	if atIdx := strings.Index(s, "@"); atIdx >= 0 {
		s = s[atIdx+1:]
	}
	// Strip query string / path
	if slashIdx := strings.Index(s, "/"); slashIdx >= 0 {
		s = s[:slashIdx]
	}
	if colonIdx := strings.LastIndex(s, ":"); colonIdx >= 0 {
		return s[colonIdx+1:]
	}
	return "27017"
}

// probeMongosReachable attempts a connect + ping with a hard deadline.
// It never touches the shared client cache — it is fire-and-forget probing only.
func probeMongosReachable(uri string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	opts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(timeout).
		SetConnectTimeout(timeout)
	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return false
	}
	defer func() { _ = c.Disconnect(context.Background()) }()
	return c.Ping(ctx, nil) == nil
}

// discoverMongosIPs runs a $currentOp aggregation on the config-server primary
// to collect the IP addresses of all currently-connected mongos processes.
// This is the fallback when config.mongos contains hostnames that are not
// reachable from the machine running MongoAdmin.
func discoverMongosIPs(cfgURI string) ([]string, error) {
	c, err := getClient(cfgURI)
	if err != nil {
		return nil, fmt.Errorf("discoverMongosIPs: cannot connect to config server: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The TaskExecutorPool connections are the internal mongos → configsvr links.
	pipeline := mongo.Pipeline{
		{
			{Key: "$currentOp", Value: bson.D{
				{Key: "allUsers", Value: true},
				{Key: "idleConnections", Value: true},
			}},
		},
		{
			{Key: "$match", Value: bson.D{
				{Key: "clientMetadata.driver.name", Value: primitive.Regex{
					Pattern: `^NetworkInterfaceTL-TaskExecutorPool`,
					Options: "",
				}},
			}},
		},
		{
			{Key: "$project", Value: bson.D{
				{Key: "_id", Value: 0},
				{Key: "ip", Value: bson.D{
					{Key: "$arrayElemAt", Value: bson.A{
						bson.D{{Key: "$split", Value: bson.A{"$client", ":"}}},
						0,
					}},
				}},
			}},
		},
		{
			{Key: "$group", Value: bson.D{{Key: "_id", Value: "$ip"}}},
		},
	}

	cursor, err := c.Database("admin").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("discoverMongosIPs: aggregation failed: %w", err)
	}
	defer cursor.Close(ctx)

	var ips []string
	for cursor.Next(ctx) {
		var doc bson.D
		if err2 := cursor.Decode(&doc); err2 != nil {
			continue
		}
		for _, e := range doc {
			if e.Key == "_id" {
				if ip, ok := e.Value.(string); ok && ip != "" {
					ips = append(ips, ip)
				}
			}
		}
	}
	return ips, cursor.Err()
}

func parseRSUri(host string) (string, string) {
	if idx := strings.Index(host, "/"); idx >= 0 {
		rsName := host[:idx]
		hosts := host[idx+1:]
		return rsName, "mongodb://" + hosts + "/?replicaSet=" + rsName
	}
	return "", "mongodb://" + host
}

// discoverTopology inspects a mongos and returns all node groups.
// Returns an error if the URI is not a mongos (so caller knows it's a plain RS).
func discoverTopology(mongosURI string) ([]NodeInfo, error) {
	// Extract credentials from the mongos URI to inject into shard URIs
	username, password := parseCredentials(mongosURI)
	
	shRes, err := runCmd(mongosURI, "admin", bson.D{{Key: "listShards", Value: 1}})
	if err != nil {
		return nil, err
	}

	var nodes []NodeInfo

	// 1. Shard replica sets
	for _, s := range toSlice(shRes["shards"]) {
		m := toMap(s)
		if m == nil {
			continue
		}
		id, _ := m["_id"].(string)
		host, _ := m["host"].(string)
		_, rUri := parseRSUri(host)
		// Inject credentials into the shard URI
		rUri = injectCredentials(rUri, username, password)
		nodes = append(nodes, NodeInfo{ID: id, Role: "shard", URI: rUri})
	}

	// 2. Config server RS — from getCmdLineOpts --configdb
	cfgURI := ""
	if opts, err2 := runCmd(mongosURI, "admin", bson.D{{Key: "getCmdLineOpts", Value: 1}}); err2 == nil {
		if parsed := toMap(opts["parsed"]); parsed != nil {
			if sharding := toMap(parsed["sharding"]); sharding != nil {
				if cs, _ := sharding["configDB"].(string); cs != "" {
					_, cfgURI = parseRSUri(cs)
				}
			}
		}
	}
	// Fallback: admin.system.version shardingVersion doc
	if cfgURI == "" {
		if vDoc, err2 := runCmd(mongosURI, "admin", bson.D{
			{Key: "find", Value: "system.version"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: "shardingVersion"}}},
		}); err2 == nil {
			if cursor := toMap(vDoc["cursor"]); cursor != nil {
				for _, row := range toSlice(cursor["firstBatch"]) {
					if doc := toMap(row); doc != nil {
						if cs, _ := doc["configsvrConnectionString"].(string); cs != "" {
							_, cfgURI = parseRSUri(cs)
						}
					}
				}
			}
		}
	}
	if cfgURI != "" {
		// Inject credentials into config server URI
		cfgURI = injectCredentials(cfgURI, username, password)
		nodes = append(nodes, NodeInfo{ID: "configsvr", Role: "configsvr", URI: cfgURI})
	}

	// 3. Mongos instances — from config.mongos collection
	//
	// Some environments advertise hostnames that are not resolvable from the
	// machine running MongoAdmin (e.g. internal Kubernetes pod names).  We
	// therefore probe each hostname with a 1-second hard timeout and, for any
	// that fail, fall back to IP-address discovery via a $currentOp aggregation
	// executed on the config-server primary.

	type mongosCandidate struct {
		id   string
		host string // "hostname:port" as stored in config.mongos._id
	}
	var candidates []mongosCandidate

	if mRes, err2 := runCmd(mongosURI, "config", bson.D{
		{Key: "find", Value: "mongos"},
		{Key: "limit", Value: 50},
	}); err2 == nil {
		if cursor := toMap(mRes["cursor"]); cursor != nil {
			for i, row := range toSlice(cursor["firstBatch"]) {
				m := toMap(row)
				if m == nil {
					continue
				}
				host, _ := m["_id"].(string)
				candidates = append(candidates, mongosCandidate{
					id:   fmt.Sprintf("mongos-%d", i),
					host: host,
				})
			}
		}
	}

	// Probe all candidate mongos concurrently with a 1-second deadline.
	type probeResult struct {
		candidate mongosCandidate
		reachable bool
	}
	probeResults := make([]probeResult, len(candidates))
	var probeWG sync.WaitGroup
	for i, cand := range candidates {
		probeWG.Add(1)
		go func(i int, cand mongosCandidate) {
			defer probeWG.Done()
			probeURI := injectCredentials("mongodb://"+cand.host, username, password)
			ok := probeMongosReachable(probeURI, 1*time.Second)
			if !ok {
				debugLog("mongos %s unreachable via hostname — will attempt IP fallback", cand.host)
			}
			probeResults[i] = probeResult{candidate: cand, reachable: ok}
		}(i, cand)
	}
	probeWG.Wait()

	// Partition into reachable (add immediately) and failed (need IP fallback).
	var failedCandidates []mongosCandidate
	failedPortSet := map[string]bool{}
	var failedPorts []string

	for _, pr := range probeResults {
		if pr.reachable {
			nodes = append(nodes, NodeInfo{
				ID:   pr.candidate.id,
				Role: "mongos",
				URI:  injectCredentials("mongodb://"+pr.candidate.host, username, password),
			})
		} else {
			failedCandidates = append(failedCandidates, pr.candidate)
			p := extractPort(pr.candidate.host)
			if !failedPortSet[p] {
				failedPortSet[p] = true
				failedPorts = append(failedPorts, p)
			}
		}
	}

	// IP-fallback: if any hostname was unreachable AND we have the config-server
	// URI, ask the config primary which IPs are currently connected as mongos.
	if len(failedCandidates) > 0 && cfgURI != "" {
		fallbackIPs, ipErr := discoverMongosIPs(cfgURI)
		if ipErr != nil {
			debugLog("mongos IP fallback aggregation error: %v", ipErr)
		} else {
			debugLog("mongos IP fallback: discovered %d IP(s): %v", len(fallbackIPs), fallbackIPs)
		}

		// For each discovered IP, try every port we saw in the failed set.
		// The first port that connects is used; a working IP:port replaces one
		// failed hostname entry (matched by position to keep IDs stable).
		ipNodeIdx := 0
		for _, ip := range fallbackIPs {
			for _, port := range failedPorts {
				candidateURI := injectCredentials(
					"mongodb://"+ip+":"+port,
					username, password,
				)
				if probeMongosReachable(candidateURI, 1*time.Second) {
					id := fmt.Sprintf("mongos-%d", ipNodeIdx)
					if ipNodeIdx < len(failedCandidates) {
						// Reuse the original sequential ID so the UI stays stable.
						id = failedCandidates[ipNodeIdx].id
					}
					debugLog("mongos IP fallback: using %s:%s (id=%s)", ip, port, id)
					nodes = append(nodes, NodeInfo{
						ID:   id,
						Role: "mongos",
						URI:  candidateURI,
					})
					ipNodeIdx++
					break // found a working port for this IP; move to next IP
				}
			}
		}

		if ipNodeIdx == 0 && len(fallbackIPs) > 0 {
			debugLog("mongos IP fallback: IPs discovered but none responded on ports %v", failedPorts)
		}
	}

	// Pre-connect all RS URIs in background
	for _, n := range nodes {
		if n.Role != "mongos" {
			go func(u string) { _, _ = getClient(u) }(n.URI)
		}
	}
	return nodes, nil
}

func handleTopology(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	nodes, err := discoverTopology(uri)
	if err != nil {
		jsonOK(w, map[string]interface{}{"type": "replicaset", "nodes": []interface{}{}})
		return
	}
	jsonOK(w, map[string]interface{}{"type": "sharded", "nodes": nodes, "mongosURI": uri})
}

// rsNodes returns nodes with roles "shard" or "configsvr" (have replSetGetStatus).
func rsNodes(nodes []NodeInfo) []NodeInfo {
	var out []NodeInfo
	for _, n := range nodes {
		if n.Role == "shard" || n.Role == "configsvr" {
			out = append(out, n)
		}
	}
	return out
}

// ── RS Status (fan-out, RS nodes only — never mongos) ─────────────────────────

func handleAllRsStatus(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	rsURIs := r.Form["rs_uri"] // only RS URIs sent by frontend
	if len(rsURIs) == 0 {
		// Plain RS mode: use the primary URI directly
		rsURIs = []string{formURI(r)}
	}

	// Parse per-shard credentials from form
	shardCreds := parseShardCredentials(r)

	type rsResult struct {
		ShardID string                 `json:"shardId"`
		URI     string                 `json:"uri"`
		Data    map[string]interface{} `json:"data,omitempty"`
		Error   string                 `json:"error,omitempty"`
	}

	var mu sync.Mutex
	var results []rsResult
	var wg sync.WaitGroup
	seen := map[string]bool{}

	for _, u := range rsURIs {
		if seen[u] {
			continue
		}
		seen[u] = true
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			// Apply shard-specific credentials if available
			finalURI := applyShardCredentials(u, shardCreds)
			
			res, err := runCmd(finalURI, "admin", bson.D{{Key: "replSetGetStatus", Value: 1}})
			rr := rsResult{URI: u}
			if err != nil {
				rr.Error = err.Error()
				rr.ShardID = u
				// Detect authentication errors
				if strings.Contains(err.Error(), "Unauthorized") || strings.Contains(err.Error(), "authentication failed") {
					debugLog("Auth failed on shard: %s", u)
				}
			} else {
				if set, ok := res["set"].(string); ok {
					rr.ShardID = set
				} else {
					rr.ShardID = u
				}
				rr.Data = res
			}
			mu.Lock()
			results = append(results, rr)
			mu.Unlock()
		}(u)
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].ShardID < results[j].ShardID })
	jsonOK(w, results)
}

// ── RS Config ─────────────────────────────────────────────────────────────────

func handleRsConfig(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	originalUri := uri // Keep original for error reporting
	
	// Parse per-shard credentials from form
	shardCreds := parseShardCredentials(r)
	// Apply shard-specific credentials if available
	uri = applyShardCredentials(uri, shardCreds)
	
	res, err := runCmd(uri, "admin", bson.D{{Key: "replSetGetConfig", Value: 1}})
	if err != nil {
		// Check if it's an auth error
		if strings.Contains(err.Error(), "Unauthorized") || strings.Contains(err.Error(), "authentication failed") {
			shardID := extractShardID(originalUri)
			jsonErrWithShard(w, err.Error(), shardID, originalUri, 401)
			return
		}
		jsonErr(w, err.Error(), 500)
		return
	}
	if cfg, ok := res["config"]; ok {
		jsonOK(w, cfg)
		return
	}
	jsonOK(w, res)
}

// ── Sharding status + controls ────────────────────────────────────────────────

type BalancerRound struct {
	Time    string                 `json:"time"`
	Error   bool                   `json:"error"`
	Details map[string]interface{} `json:"details"`
}

type MigResult struct {
	Note  string `json:"note"`
	Count int    `json:"count"`
}

func handleShStatus(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)

	nodes, err := discoverTopology(uri)
	if err != nil {
		jsonOK(w, map[string]interface{}{"notSharded": true, "error": err.Error()})
		return
	}

	balancer, _ := runCmd(uri, "admin", bson.D{{Key: "balancerStatus", Value: 1}})
	dbs, _ := runCmd(uri, "admin", bson.D{{Key: "listDatabases", Value: 1}})
	rwcRes, _ := runCmd(uri, "admin", bson.D{{Key: "getDefaultRWConcern", Value: 1}})

	// ── Balancer round history (last 5 from config.actionlog) ──
	var balRounds []BalancerRound
	if brRes, err2 := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "actionlog"},
		{Key: "filter", Value: bson.D{{Key: "what", Value: "balancer.round"}}},
		{Key: "sort", Value: bson.D{{Key: "time", Value: -1}}},
		{Key: "limit", Value: 5},
	}); err2 == nil {
		if cursor := toMap(brRes["cursor"]); cursor != nil {
			for _, row := range toSlice(cursor["firstBatch"]) {
				m := toMap(row)
				if m == nil {
					continue
				}
				br := BalancerRound{Details: toMap(m["details"])}
				if br.Details != nil {
					if e, ok := br.Details["errorOccured"].(bool); ok {
						br.Error = e
					}
				}
				// time field is a primitive.DateTime → already converted to RFC3339 string
				br.Time, _ = m["time"].(string)
				balRounds = append(balRounds, br)
			}
		}
	}
	// Count failed in last 5
	failedRounds := 0
	for _, br := range balRounds {
		if br.Error {
			failedRounds++
		}
	}

	// ── Migration results last 24 h (config.changelog, what=moveChunk.from) ──
	cutoff := primitive.NewDateTimeFromTime(time.Now().Add(-24 * time.Hour))
	migCounts := map[string]int{}
	if mrRes, err2 := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "changelog"},
		{Key: "filter", Value: bson.D{
			{Key: "what", Value: "moveChunk.from"},
			{Key: "time", Value: bson.D{{Key: "$gt", Value: cutoff}}},
		}},
		{Key: "limit", Value: 2000},
	}); err2 == nil {
		if cursor := toMap(mrRes["cursor"]); cursor != nil {
			for _, row := range toSlice(cursor["firstBatch"]) {
				m := toMap(row)
				if m == nil {
					continue
				}
				note := "unknown"
				if details := toMap(m["details"]); details != nil {
					if n, ok := details["note"].(string); ok && n != "" {
						note = n
					}
				}
				migCounts[note]++
			}
		}
	}
	var migResults []MigResult
	for note, cnt := range migCounts {
		migResults = append(migResults, MigResult{Note: note, Count: cnt})
	}
	sort.Slice(migResults, func(i, j int) bool { return migResults[i].Count > migResults[j].Count })
	if len(migResults) == 0 {
		migResults = []MigResult{{Note: "No migrations in last 24h", Count: 0}}
	}

	jsonOK(w, map[string]interface{}{
		"nodes":        nodes,
		"balancer":     balancer,
		"dbs":          dbs,
		"rwconcern":    rwcRes,
		"balRounds":    balRounds,
		"failedRounds": failedRounds,
		"migResults":   migResults,
	})
}

func handleBalancerToggle(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	enable := r.FormValue("enable") == "true"
	cmd := bson.D{{Key: "balancerStop", Value: 1}}
	if enable {
		cmd = bson.D{{Key: "balancerStart", Value: 1}}
	}
	res, err := runCmd(uri, "admin", cmd)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, res)
}

func handleSetWriteConcern(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	wStr := r.FormValue("w")
	jStr := r.FormValue("j")
	wtStr := r.FormValue("wtimeout")

	wc := bson.D{}
	if wStr != "" {
		var wNum int
		if n, err := fmt.Sscanf(wStr, "%d", &wNum); n == 1 && err == nil {
			wc = append(wc, bson.E{Key: "w", Value: wNum})
		} else {
			wc = append(wc, bson.E{Key: "w", Value: wStr})
		}
	}
	if jStr != "" {
		wc = append(wc, bson.E{Key: "j", Value: jStr == "true"})
	}
	if wtStr != "" {
		var wt int64
		fmt.Sscanf(wtStr, "%d", &wt)
		wc = append(wc, bson.E{Key: "wtimeout", Value: wt})
	}

	res, err := runCmd(uri, "admin", bson.D{
		{Key: "setDefaultRWConcern", Value: 1},
		{Key: "defaultWriteConcern", Value: wc},
	})
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, res)
}

// ── Flow Control — setParameter on individual mongod (not mongos) ─────────────

func handleFlowControl(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	enable := r.FormValue("enable")       // "true" | "false" | ""
	lagStr := r.FormValue("targetLagSecs") // "" or integer string

	// MongoDB 4.2+: enableFlowControl and flowControlTargetLagSeconds are
	// top-level setParameter keys, not nested under "flowControl".
	cmd := bson.D{{Key: "setParameter", Value: 1}}
	if enable != "" {
		cmd = append(cmd, bson.E{Key: "enableFlowControl", Value: enable == "true"})
	}
	if lagStr != "" {
		var lag int
		fmt.Sscanf(lagStr, "%d", &lag)
		cmd = append(cmd, bson.E{Key: "flowControlTargetLagSeconds", Value: lag})
	}
	if len(cmd) == 1 {
		jsonErr(w, "no parameters specified", 400)
		return
	}
	res, err := runCmd(uri, "admin", cmd)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, res)
}

// ── DB Stats ──────────────────────────────────────────────────────────────────

type ShardDist struct {
	ShardID     string  `json:"shardId"`
	DataSize    int64   `json:"dataSize"`
	StorageSize int64   `json:"storageSize"`
	Objects     int64   `json:"objects"`
	DataPct     float64 `json:"dataPct"`
	ObjPct      float64 `json:"objPct"`
}

// ── Chunk Distribution ─────────────────────────────────────────────────────────

type ChunkDist struct {
	ShardID    string  `json:"shardId"`
	ChunkCount int     `json:"chunkCount"`
	ChunkPct   float64 `json:"chunkPct"`
	DataSize   int64   `json:"dataSize"`
	Objects    int64   `json:"objects"`
	DataPct    float64 `json:"dataPct"`
	ObjPct     float64 `json:"objPct"`
}

type ChunkRange struct {
	ShardID string      `json:"shardId"`
	Min     interface{} `json:"min"`
	Max     interface{} `json:"max"`
}

type ChunkDistResult struct {
	Namespace   string       `json:"namespace"`
	ShardKey    string       `json:"shardKey"`
	TotalChunks int          `json:"totalChunks"`
	Distribution []ChunkDist `json:"distribution"`
	Chunks      []ChunkRange `json:"chunks"` // top N chunk boundaries
	BalanceScore float64     `json:"balanceScore"` // 0-1: 1=perfectly balanced, 0=highly imbalanced
}

type DBStat struct {
	Name               string      `json:"name"`
	SizeOnDisk         int64       `json:"sizeOnDisk"`
	DataSize           int64       `json:"dataSize"`
	StorageSize        int64       `json:"storageSize"`
	IndexSize          int64       `json:"indexSize"`
	Objects            int64       `json:"objects"`
	Collections        int64       `json:"collections"`        // accurate: from listCollections
	ShardedCollections int64       `json:"shardedCollections"` // from config.collections
	Indexes            int64       `json:"indexes"`
	AvgObjSize         float64     `json:"avgObjSize"`
	PrimaryShard       string      `json:"primaryShard"`
	IsPartitioned      bool        `json:"isPartitioned"`
	ShardDist          []ShardDist `json:"shardDist"`
}

func handleShardedCollections(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	
	if dbName == "" {
		jsonErr(w, "db required", 400)
		return
	}
	
	// Query config.collections for this database
	collRes, err := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "collections"},
		{Key: "filter", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + dbName + "\\."}}},
			{Key: "dropped", Value: bson.D{{Key: "$ne", Value: true}}},
		}},
		{Key: "limit", Value: 1000},
	})
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	
	var collections []string
	if cursor := toMap(collRes["cursor"]); cursor != nil {
		for _, row := range toSlice(cursor["firstBatch"]) {
			if m := toMap(row); m != nil {
				if id, ok := m["_id"].(string); ok {
					// Extract collection name from namespace
					parts := strings.SplitN(id, ".", 2)
					if len(parts) == 2 && parts[0] == dbName {
						collections = append(collections, parts[1])
					}
				}
			}
		}
	}
	
	sort.Strings(collections)
	jsonOK(w, map[string]interface{}{"collections": collections})
}

func handleChunkDistribution(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	originalUri := uri // Keep original for error reporting
	ns := strings.TrimSpace(r.FormValue("ns")) // "dbName.collName"
	
	// Parse per-shard credentials from form
	shardCreds := parseShardCredentials(r)
	// Apply shard-specific credentials if available
	uri = applyShardCredentials(uri, shardCreds)
	
	if ns == "" {
		jsonErr(w, "ns (namespace) required", 400)
		return
	}
	
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) != 2 {
		jsonErr(w, "Invalid namespace", 400)
		return
	}
	
	result := ChunkDistResult{
		Namespace: ns,
		Distribution: []ChunkDist{},
		Chunks: []ChunkRange{},
	}
	
	// Get collection info from config.collections to verify it's sharded and get shard key
	collRes, err := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "collections"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: ns}}},
	})
	if err != nil {
		// Check if it's an auth error and extract shard ID if possible
		if strings.Contains(err.Error(), "Unauthorized") || strings.Contains(err.Error(), "authentication failed") {
			shardID := extractShardID(originalUri)
			jsonErrWithShard(w, err.Error(), shardID, originalUri, 401)
			return
		}
		jsonErr(w, "Collection not found or not sharded: "+ns, 404)
		return
	}
	
	if cursor := toMap(collRes["cursor"]); cursor != nil {
		found := false
		for _, row := range toSlice(cursor["firstBatch"]) {
			if m := toMap(row); m != nil {
				// Get shard key
				if keyVal := m["key"]; keyVal != nil {
					if keyMap, ok := keyVal.(map[string]interface{}); ok {
						var parts []string
						for k := range keyMap {
							parts = append(parts, k)
						}
						sort.Strings(parts)
						result.ShardKey = strings.Join(parts, ", ")
					}
				}
				found = true
				break
			}
		}
		if !found {
			jsonErr(w, "Collection not found or not sharded: "+ns, 404)
			return
		}
	}
	
	debugLog("Collection %s found, Shard Key: %s", ns, result.ShardKey)
	
	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	// Step 1: Get per-shard data distribution using $shardedDataDistribution
	shardData := make(map[string]ShardDist)
	totalData := int64(0)
	totalObjs := int64(0)
	
	pipeline := bson.A{
		bson.D{{Key: "$shardedDataDistribution", Value: bson.D{}}},
		bson.D{{Key: "$match", Value: bson.D{{Key: "ns", Value: ns}}}},
	}
	
	cursor, err := c.Database("admin").Aggregate(ctx, pipeline)
	if err != nil {
		debugLog("Error in $shardedDataDistribution: %v", err)
		jsonErr(w, "Failed to get shard distribution: "+err.Error(), 500)
		return
	}
	defer cursor.Close(ctx)
	
	var distResults []bson.D
	if err := cursor.All(ctx, &distResults); err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	
	// Parse the distribution results
	for _, doc := range distResults {
		distMap := bsonToAny(doc).(map[string]interface{})
		
		if shardsVal := distMap["shards"]; shardsVal != nil {
			if shardsList, ok := shardsVal.([]interface{}); ok {
				for _, shardRaw := range shardsList {
					if shardMap, ok := shardRaw.(map[string]interface{}); ok {
						shardName, _ := shardMap["shardName"].(string)
						ownedDocs := toInt64(shardMap["numOwnedDocuments"])
						ownedSize := toInt64(shardMap["ownedSizeBytes"])
						
						shardData[shardName] = ShardDist{
							ShardID:  shardName,
							DataSize: ownedSize,
							Objects:  ownedDocs,
						}
						
						totalData += ownedSize
						totalObjs += ownedDocs
						
						debugLog("Shard %s: %d docs, %d bytes", shardName, ownedDocs, ownedSize)
					}
				}
			}
		}
	}
	
	debugLog("$shardedDataDistribution found %d shards, totalData=%d, totalObjs=%d", len(shardData), totalData, totalObjs)
	
	// Step 2: Get chunk counts per shard using $lookup to join on UUID
	shardCounts := make(map[string]int)
	totalChunks := 0
	
	// Use aggregation with $lookup to properly join collections and chunks by UUID
	chunkPipeline := bson.A{
		// 1. Find the specific collection by namespace
		bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: ns}}}},
		// 2. Join with the chunks collection using the UUID
		bson.D{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "chunks"},
			{Key: "localField", Value: "uuid"},
			{Key: "foreignField", Value: "uuid"},
			{Key: "as", Value: "chunk_details"},
		}}},
		// 3. Unwind the joined array to group by shard
		bson.D{{Key: "$unwind", Value: "$chunk_details"}},
		// 4. Group by shard name and count the occurrences
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$chunk_details.shard"},
			{Key: "chunkCount", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	
	chunkCursor, err := c.Database("config").Collection("collections").Aggregate(ctx, chunkPipeline)
	if err != nil {
		debugLog("Error in chunk $lookup aggregation: %v", err)
	} else {
		defer chunkCursor.Close(ctx)
		var chunkResults []bson.D
		if err := chunkCursor.All(ctx, &chunkResults); err == nil {
			for _, doc := range chunkResults {
				chunkMap := bsonToAny(doc).(map[string]interface{})
				shardID, _ := chunkMap["_id"].(string)
				count := toInt64(chunkMap["chunkCount"])
				if shardID != "" {
					shardCounts[shardID] = int(count)
					totalChunks += int(count)
					debugLog("Shard %s has %d chunks", shardID, count)
				}
			}
		} else {
			debugLog("Error decoding chunk results: %v", err)
		}
	}
	
	debugLog("Found %d total chunks", totalChunks)
	
	result.TotalChunks = totalChunks
	
	// Build distribution from collStats per-shard data
	if len(shardData) > 0 {
		if totalData == 0 {
			totalData = 1
		}
		if totalObjs == 0 {
			totalObjs = 1
		}
		
		for shardID, shard := range shardData {
			dataPct := float64(shard.DataSize) / float64(totalData) * 100
			objPct := float64(shard.Objects) / float64(totalObjs) * 100
			
			// If we got actual chunk counts from config.chunks, use them
			chunkCount := shardCounts[shardID]
			if chunkCount == 0 {
				// Fallback: estimate chunks from data size (not accurate but better than nothing)
				chunkCount = 1
			}
			
			var chunkPct float64
			if result.TotalChunks > 0 {
				chunkPct = float64(chunkCount) / float64(result.TotalChunks) * 100
			}
			
			result.Distribution = append(result.Distribution, ChunkDist{
				ShardID:    shardID,
				ChunkCount: chunkCount,
				ChunkPct:   chunkPct,
				DataSize:   shard.DataSize,
				Objects:    shard.Objects,
				DataPct:    dataPct,
				ObjPct:     objPct,
			})
		}
		
		sort.Slice(result.Distribution, func(i, j int) bool {
			return result.Distribution[i].DataSize > result.Distribution[j].DataSize
		})
		
		// Calculate balance score based on data distribution
		avgData := float64(totalData) / float64(len(shardData))
		var variance float64
		for shardID := range shardData {
			diff := float64(shardData[shardID].DataSize) - avgData
			variance += diff * diff
		}
		variance /= float64(len(shardData))
		stdDev := math.Sqrt(variance)
		if avgData > 0 {
			result.BalanceScore = 1.0 - math.Min(1.0, stdDev/avgData)
		} else {
			result.BalanceScore = 1.0
		}
		
		debugLog("Final result: %d shards, totalData=%d, balance score: %.2f%%", len(shardData), totalData, result.BalanceScore*100)
	}
	
	jsonOK(w, result)
}

func handleDbStats(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)

	listRes, err := runCmd(uri, "admin", bson.D{{Key: "listDatabases", Value: 1}})
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	dbs := toSlice(listRes["databases"])

	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	// Fetch sharded collection set from config.collections (sharded clusters only).
	// Key: "dbName.collName" → true
	shardedColls := map[string]bool{}
	if scRes, err2 := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "collections"},
		{Key: "filter", Value: bson.D{{Key: "dropped", Value: bson.D{{Key: "$ne", Value: true}}}}},
		{Key: "limit", Value: 5000},
	}); err2 == nil {
		if cursor := toMap(scRes["cursor"]); cursor != nil {
			for _, row := range toSlice(cursor["firstBatch"]) {
				if m := toMap(row); m != nil {
					if id, ok := m["_id"].(string); ok {
						shardedColls[id] = true
					}
				}
			}
		}
	}

	// Fetch primary shard per DB from config.databases
	primaryShards := map[string]string{}
	partitioned := map[string]bool{}
	if dbConfRes, err2 := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "databases"},
		{Key: "limit", Value: 500},
	}); err2 == nil {
		if cursor := toMap(dbConfRes["cursor"]); cursor != nil {
			for _, row := range toSlice(cursor["firstBatch"]) {
				m := toMap(row)
				if m == nil {
					continue
				}
				dbName, _ := m["_id"].(string)
				primary, _ := m["primary"].(string)
				part, _ := m["partitioned"].(bool)
				primaryShards[dbName] = primary
				partitioned[dbName] = part
			}
		}
	}

	var mu sync.Mutex
	var stats []DBStat
	var wg sync.WaitGroup

	for _, dbRaw := range dbs {
		dbm := toMap(dbRaw)
		if dbm == nil {
			continue
		}
		name, _ := dbm["name"].(string)
		if name == "" {
			continue
		}
		sizeOnDisk := toInt64(dbm["sizeOnDisk"])

		wg.Add(1)
		go func(name string, sizeOnDisk int64) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			st := DBStat{
				Name:          name,
				SizeOnDisk:    sizeOnDisk,
				PrimaryShard:  primaryShards[name],
				IsPartitioned: partitioned[name],
			}

			// ── Accurate collection count via listCollections ──
			// dbStats.collections is unreliable through mongos.
			if names, err2 := c.Database(name).ListCollectionNames(ctx, bson.D{}); err2 == nil {
				st.Collections = int64(len(names))
				// Count sharded collections for this DB
				for _, colName := range names {
					if shardedColls[name+"."+colName] {
						st.ShardedCollections++
					}
				}
			}

			// ── dbStats for sizes, objects, indexes ──
			var raw bson.D
			if err2 := c.Database(name).RunCommand(ctx, bson.D{
				{Key: "dbStats", Value: 1},
				{Key: "scale", Value: 1},
			}).Decode(&raw); err2 == nil {
				m := bsonToAny(raw).(map[string]interface{})
				st.DataSize    = toInt64(m["dataSize"])
				st.StorageSize = toInt64(m["storageSize"])
				st.IndexSize   = toInt64(m["indexSize"])
				st.Objects     = toInt64(m["objects"])
				st.Indexes     = toInt64(m["indexes"])
				if v, ok := m["avgObjSize"].(float64); ok {
					st.AvgObjSize = v
				}

				// ── Per-shard distribution from raw field (mongos only) ──
				if rawShards := toMap(m["raw"]); rawShards != nil {
					totalData := st.DataSize
					totalObjs := st.Objects
					if totalData == 0 {
						totalData = 1
					}
					if totalObjs == 0 {
						totalObjs = 1
					}
					for shardKey, shardRaw := range rawShards {
						sm := toMap(shardRaw)
						if sm == nil {
							continue
						}
						// shardKey is "rsName/host:port,..." — extract rsName only
						shardID := shardKey
						if idx := strings.Index(shardKey, "/"); idx >= 0 {
							shardID = shardKey[:idx]
						}
						sd := ShardDist{
							ShardID:     shardID,
							DataSize:    toInt64(sm["dataSize"]),
							StorageSize: toInt64(sm["storageSize"]),
							Objects:     toInt64(sm["objects"]),
						}
						sd.DataPct = float64(sd.DataSize) / float64(totalData) * 100
						sd.ObjPct  = float64(sd.Objects) / float64(totalObjs) * 100
						st.ShardDist = append(st.ShardDist, sd)
					}
					sort.Slice(st.ShardDist, func(i, j int) bool {
						return st.ShardDist[i].DataSize > st.ShardDist[j].DataSize
					})
				}
			}

			mu.Lock()
			stats = append(stats, st)
			mu.Unlock()
		}(name, sizeOnDisk)
	}
	wg.Wait()
	sort.Slice(stats, func(i, j int) bool { return stats[i].DataSize > stats[j].DataSize })
	jsonOK(w, stats)
}

// ── getParameter helper ───────────────────────────────────────────────────────

func handleGetParameter(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	param := r.FormValue("param")
	if param == "" {
		jsonErr(w, "param required", 400)
		return
	}
	res, err := runCmd(uri, "admin", bson.D{
		{Key: "getParameter", Value: 1},
		{Key: param, Value: 1},
	})
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, res)
}

// ── Server status ─────────────────────────────────────────────────────────────

func handleServerStatus(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	res, err := runCmd(formURI(r), "admin", bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, res)
}

// handleServerMetrics is a lightweight alternative to handleServerStatus for
// high-frequency polling (e.g. the live traffic monitor).  The caller passes a
// comma-separated `sections` query/form value listing only the serverStatus
// top-level sections it actually needs (e.g. "opcounters,connections,network").
// Every other heavy section is explicitly suppressed in the command so the
// server skips computing and serialising it.  A full serverStatus on a busy
// replica set can exceed 500 KB; a targeted fetch of two or three sections
// typically comes back in under 3 KB.
//
// Sections that are always lightweight and returned unconditionally by MongoDB
// regardless of suppression flags (host, version, pid, uptime, localTime,
// process, asserts) are kept as-is; we only suppress the genuinely expensive
// ones.
func handleServerMetrics(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	// Parse the requested sections (comma-separated)
	raw := strings.TrimSpace(r.FormValue("sections"))
	want := map[string]bool{}
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			want[s] = true
		}
	}

	// Complete list of sections that are expensive / bulky and safe to suppress.
	// Any section NOT in this list is either always-present (host, uptime …) or
	// lightweight enough that leaving it in causes no harm.
	heavy := []string{
		"wiredTiger",
		"repl",
		"tcmalloc",
		"locks",
		"metrics",
		"logicalSessionRecordCache",
		"storageEngine",
		"transactions",
		"twoPhaseCommitCoordinator",
		"sharding",
		"catalogStats",
		"indexBuilds",
		"mirroredReads",
		"flowControl",
		"electionMetrics",
		"opLatencies",
		"trafficRecording",
		"queryAnalyzers",
		"changeStreamPreImages",
		"querySettings",
	}

	cmd := bson.D{{Key: "serverStatus", Value: 1}}
	for _, sec := range heavy {
		if !want[sec] {
			cmd = append(cmd, bson.E{Key: sec, Value: 0})
		}
	}

	res, err := runCmd(formURI(r), "admin", cmd)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	// Return only the requested sections plus the always-present lightweight
	// fields so callers don't have to deal with unexpected keys.
	alwaysKeep := map[string]bool{
		"host": true, "version": true, "process": true,
		"pid": true, "uptime": true, "uptimeMillis": true,
		"localTime": true, "ok": true,
	}
	if len(want) > 0 {
		filtered := map[string]interface{}{}
		for k, v := range res {
			if want[k] || alwaysKeep[k] {
				filtered[k] = v
			}
		}
		jsonOK(w, filtered)
		return
	}
	jsonOK(w, res)
}

func handleHostInfo(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)

	// Fetch hostInfo and serverStatus in parallel for memory data
	type result struct {
		hi  map[string]interface{}
		ss  map[string]interface{}
		err error
	}
	hiCh := make(chan result, 1)
	ssCh := make(chan result, 1)

	go func() {
		res, err := runCmd(uri, "admin", bson.D{{Key: "hostInfo", Value: 1}})
		hiCh <- result{hi: res, err: err}
	}()
	go func() {
		res, err := runCmd(uri, "admin", bson.D{{Key: "serverStatus", Value: 1}})
		ssCh <- result{ss: res, err: err}
	}()
	hiRes := <-hiCh
	ssRes := <-ssCh

	if hiRes.err != nil {
		jsonErr(w, hiRes.err.Error(), 500)
		return
	}

	system := toMap(hiRes.hi["system"])
	osMap  := toMap(hiRes.hi["os"])
	extra  := toMap(hiRes.hi["extra"])

	out := map[string]interface{}{}
	if system != nil {
		out["hostname"]    = system["hostname"]
		out["cpuAddrSize"] = system["cpuAddrSize"]
		out["memSizeMB"]   = system["memSizeMB"]
		out["numCores"]    = system["numCores"]
		out["cpuArch"]     = system["cpuArch"]
		out["numaEnabled"] = system["numaEnabled"]
	}
	if osMap != nil {
		out["osType"]    = osMap["type"]
		out["osName"]    = osMap["name"]
		out["osVersion"] = osMap["version"]
	}
	if extra != nil {
		// cpuString may live in extra or extra.extra depending on OS/version
		cpuStr := extra["cpuString"]
		if cpuStr == nil {
			if inner := toMap(extra["extra"]); inner != nil {
				cpuStr = inner["cpuString"]
			}
		}
		out["cpuString"]    = cpuStr
		out["cpuFeatures"]  = extra["cpuFeatures"]
		out["maxOpenFiles"] = extra["maxOpenFiles"]
		// numPages × pageSize = total physical memory from OS perspective
		out["numPages"]     = extra["numPages"]
		out["pageSize"]     = extra["pageSize"]
		// Free memory: available directly on some platforms
		if extra["freeRamMB"] != nil    { out["freeRamMB"]    = extra["freeRamMB"] }
		if extra["availableMB"] != nil  { out["freeRamMB"]    = extra["availableMB"] }
	}

	// Add serverStatus.mem for resident/virtual and extra_info for Linux heap
	if ssRes.err == nil {
		if mem := toMap(ssRes.ss["mem"]); mem != nil {
			out["residentMB"] = mem["resident"]
			out["virtualMB"]  = mem["virtual"]
		}
		// extra_info on Linux has heap_usage_bytes
		if ei := toMap(ssRes.ss["extra_info"]); ei != nil {
			out["heapUsageBytes"] = ei["heap_usage_bytes"]
		}
	}

	jsonOK(w, out)
}

type CmdMetric struct {
	Name   string `json:"Name"`
	Total  int64  `json:"Total"`
	Failed int64  `json:"Failed"`
}

func handleTopCommands(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	res, err := runCmd(formURI(r), "admin", bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	metrics, _ := res["metrics"].(map[string]interface{})
	cmdsRaw, _ := metrics["commands"].(map[string]interface{})
	var list []CmdMetric
	for name, v := range cmdsRaw {
		if name == "<UNKNOWN>" {
			continue
		}
		m := toMap(v)
		if m == nil {
			continue
		}
		total := toInt64(m["total"])
		if total == 0 {
			continue
		}
		list = append(list, CmdMetric{Name: name, Total: total, Failed: toInt64(m["failed"])})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Total > list[j].Total })
	if len(list) > 25 {
		list = list[:25]
	}
	jsonOK(w, list)
}

// ── Current ops ───────────────────────────────────────────────────────────────

func handleCurrentOp(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pipeline := bson.A{
		bson.D{{Key: "$currentOp", Value: bson.D{
			{Key: "allUsers", Value: true},
			{Key: "idleConnections", Value: true},
			{Key: "idleCursors", Value: true},
			{Key: "idleSessions", Value: true},
			{Key: "localOps", Value: true},
		}}},
	}
	cursor, err := c.Database("admin").Aggregate(ctx, pipeline)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	defer cursor.Close(ctx)
	var rawOps []bson.D
	if err := cursor.All(ctx, &rawOps); err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	ops := make([]interface{}, len(rawOps))
	for i, op := range rawOps {
		ops[i] = bsonToAny(op)
	}
	jsonOK(w, map[string]interface{}{"inprog": ops, "ok": 1})
}

// ── Patch RS member ───────────────────────────────────────────────────────────

func handlePatchMember(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, "POST only", 405)
		return
	}
	_ = r.ParseForm()
	uri := formURI(r)
	memberIDStr := r.FormValue("member_id")
	if memberIDStr == "" {
		jsonErr(w, "member_id required", 400)
		return
	}
	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var cfgResp bson.D
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetConfig", Value: 1}}).Decode(&cfgResp); err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	raw := bsonToAny(cfgResp).(map[string]interface{})
	cfg := toMap(raw["config"])
	if cfg == nil {
		cfg = raw
	}

	members := toSlice(cfg["members"])
	var target map[string]interface{}
	for _, m := range members {
		mm := toMap(m)
		if mm != nil && fmt.Sprintf("%v", mm["_id"]) == memberIDStr {
			target = mm
			break
		}
	}
	if target == nil {
		jsonErr(w, "member not found", 404)
		return
	}

	if v := r.FormValue("priority"); v != "" {
		var f float64
		fmt.Sscanf(v, "%f", &f)
		target["priority"] = f
	}
	if v := r.FormValue("hidden"); v != "" {
		target["hidden"] = v == "true"
	}
	if v := r.FormValue("votes"); v != "" {
		var i int
		fmt.Sscanf(v, "%d", &i)
		target["votes"] = i
	}
	if v := r.FormValue("secondaryDelaySecs"); v != "" {
		var i int64
		fmt.Sscanf(v, "%d", &i)
		target["secondaryDelaySecs"] = i
	}
	if ver, ok := cfg["version"].(int32); ok {
		cfg["version"] = ver + 1
	}
	cfgJSON, _ := json.Marshal(cfg)
	var newCfg bson.D
	if err := bson.UnmarshalExtJSON(cfgJSON, true, &newCfg); err != nil {
		jsonErr(w, "re-encode: "+err.Error(), 500)
		return
	}
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetReconfig", Value: newCfg}}).Err(); err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, map[string]string{"ok": "1", "message": "Member " + memberIDStr + " reconfigured"})
}

// ── User & Role Management ────────────────────────────────────────────────────

type RoleRef struct {
	Role string `json:"role"`
	Db   string `json:"db"`
}

type RoleInfo struct {
	Name           string    `json:"name"`
	Database       string    `json:"db"`
	InheritedRoles []RoleRef `json:"inheritedRoles"`
	PrivilegeCount int       `json:"privilegeCount"`
	IsBuiltin      bool      `json:"isBuiltin"`
}

type UserInfo struct {
	Username string   `json:"username"`
	Database string   `json:"db"`
	Roles    []string `json:"roles"`
}

func handleGetRoles(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	showBuiltinStr := r.FormValue("showBuiltinRoles")
	showPrivilegesStr := r.FormValue("showPrivileges")
	
	if dbName == "" {
		jsonErr(w, "db required", 400)
		return
	}
	
	showBuiltin := showBuiltinStr != "false"
	showPrivileges := showPrivilegesStr == "true"
	
	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	
	debugLog(fmt.Sprintf("handleGetRoles: uri=%s, db=%s, showBuiltin=%v, showPrivileges=%v", maskURI(uri), dbName, showBuiltin, showPrivileges))
	
	c, err := getClient(uri)
	if err != nil {
		debugLog(fmt.Sprintf("handleGetRoles: getClient error: %v", err))
		jsonErr(w, err.Error(), 500)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	var result bson.D
	err = c.Database(dbName).RunCommand(ctx, bson.D{
		{Key: "rolesInfo", Value: 1},
		{Key: "showBuiltinRoles", Value: showBuiltin},
		{Key: "showPrivileges", Value: showPrivileges},
	}).Decode(&result)
	
	if err != nil {
		debugLog(fmt.Sprintf("handleGetRoles: rolesInfo command error: %v", err))
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	
	// Convert BSON to regular Go types
	resultMap := bsonToAny(result).(map[string]interface{})
	debugLog(fmt.Sprintf("handleGetRoles: result type: %T, keys: %v", resultMap, len(resultMap)))
	
	var roles []RoleInfo
	if rolesRaw, ok := resultMap["roles"]; ok {
		debugLog(fmt.Sprintf("handleGetRoles: roles field found, type: %T", rolesRaw))
		if rolesList, ok := rolesRaw.([]interface{}); ok {
			debugLog(fmt.Sprintf("handleGetRoles: found %d roles", len(rolesList)))
			for _, r := range rolesList {
				if rm, ok := r.(map[string]interface{}); ok {
					inheritedRoles := []RoleRef{}
					if ir, ok := rm["inheritedRoles"]; ok {
						if irList, ok := ir.([]interface{}); ok {
							for _, irItem := range irList {
								if irMap, ok := irItem.(map[string]interface{}); ok {
									role, _ := irMap["role"].(string)
									db2, _  := irMap["db"].(string)
									if role != "" {
										inheritedRoles = append(inheritedRoles, RoleRef{Role: role, Db: db2})
									}
								}
							}
						}
					}
					
					privilegeCount := 0
					if priv, ok := rm["privileges"]; ok {
						if privList, ok := priv.([]interface{}); ok {
							privilegeCount = len(privList)
						}
					}
					
					isBuiltin := false
					if ib, ok := rm["isBuiltin"].(bool); ok {
						isBuiltin = ib
					}
					
					name, _ := rm["role"].(string)
					roles = append(roles, RoleInfo{
						Name:           name,
						Database:       dbName,
						InheritedRoles: inheritedRoles,
						PrivilegeCount: privilegeCount,
						IsBuiltin:      isBuiltin,
					})
				}
			}
		} else {
			debugLog(fmt.Sprintf("handleGetRoles: roles is not []interface{}, type: %T", rolesRaw))
		}
	} else {
		debugLog("handleGetRoles: no 'roles' field in result")
	}
	
	debugLog(fmt.Sprintf("handleGetRoles: returning %d roles", len(roles)))
	sort.Slice(roles, func(i, j int) bool { return roles[i].Name < roles[j].Name })
	if roles == nil {
		roles = []RoleInfo{}
	}
	jsonOK(w, roles)
}

// handleConnectionStatus — POST /api/user/connection-status
// Returns the currently authenticated identity and effective roles as seen
// by the connected server, via db.runCommand({connectionStatus: 1,
// showPrivileges: true}). The frontend uses this to show a "Your Session"
// card so the DBA can verify which identity their actions are running as
// before they create/sync users or roles.
func handleConnectionStatus(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res bson.D
	// showPrivileges adds the fully-resolved inheritedPrivileges array, which
	// is what surfaces to the UI as "what can this session actually do" —
	// otherwise we'd only see role names and have to chase them ourselves.
	err = c.Database("admin").RunCommand(ctx, bson.D{
		{Key: "connectionStatus", Value: 1},
		{Key: "showPrivileges",   Value: true},
	}).Decode(&res)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	jsonOK(w, bsonToAny(res))
}

func handleGetUsers(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	
	if dbName == "" {
		jsonErr(w, "db required", 400)
		return
	}
	
	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	
	debugLog(fmt.Sprintf("handleGetUsers: uri=%s, db=%s", maskURI(uri), dbName))
	
	c, err := getClient(uri)
	if err != nil {
		debugLog(fmt.Sprintf("handleGetUsers: getClient error: %v", err))
		jsonErr(w, err.Error(), 500)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	var result bson.D
	err = c.Database(dbName).RunCommand(ctx, bson.D{
		{Key: "usersInfo", Value: 1},
	}).Decode(&result)
	
	if err != nil {
		debugLog(fmt.Sprintf("handleGetUsers: usersInfo command error: %v", err))
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	
	// Convert BSON to regular Go types
	resultMap := bsonToAny(result).(map[string]interface{})
	debugLog(fmt.Sprintf("handleGetUsers: result type: %T, keys: %v", resultMap, len(resultMap)))
	
	var users []UserInfo
	if usersRaw, ok := resultMap["users"]; ok {
		debugLog(fmt.Sprintf("handleGetUsers: users field found, type: %T", usersRaw))
		if usersList, ok := usersRaw.([]interface{}); ok {
			debugLog(fmt.Sprintf("handleGetUsers: found %d users", len(usersList)))
			for _, u := range usersList {
				if um, ok := u.(map[string]interface{}); ok {
					roles := []string{}
					if rolesRaw, ok := um["roles"]; ok {
						if rolesList, ok := rolesRaw.([]interface{}); ok {
							for _, role := range rolesList {
								if roleMap, ok := role.(map[string]interface{}); ok {
									if roleName, ok := roleMap["role"].(string); ok {
										roles = append(roles, roleName)
									}
								}
							}
						}
					}
					
					username, _ := um["user"].(string)
					users = append(users, UserInfo{
						Username: username,
						Database: dbName,
						Roles:    roles,
					})
				}
			}
		} else {
			debugLog(fmt.Sprintf("handleGetUsers: users is not []interface{}, type: %T", usersRaw))
		}
	} else {
		debugLog("handleGetUsers: no 'users' field in result")
	}
	
	debugLog(fmt.Sprintf("handleGetUsers: returning %d users", len(users)))
	sort.Slice(users, func(i, j int) bool { return users[i].Username < users[j].Username })
	if users == nil {
		users = []UserInfo{}
	}
	jsonOK(w, users)
}

func handleCreateRole(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	roleName := strings.TrimSpace(r.FormValue("roleName"))
	dbName := strings.TrimSpace(r.FormValue("db"))
	privilegesJSON := r.FormValue("privileges") // JSON string
	inheritedRolesJSON := r.FormValue("inheritedRoles") // JSON string
	
	if roleName == "" || dbName == "" {
		jsonErr(w, "roleName and db required", 400)
		return
	}
	
	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	
	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	// Parse privileges from JSON
	var privileges []interface{}
	if privilegesJSON != "" {
		if err := json.Unmarshal([]byte(privilegesJSON), &privileges); err != nil {
			jsonErr(w, "invalid privileges JSON: "+err.Error(), 400)
			return
		}
	}
	
	// Parse inherited roles from JSON
	var inheritedRoles []interface{}
	if inheritedRolesJSON != "" {
		if err := json.Unmarshal([]byte(inheritedRolesJSON), &inheritedRoles); err != nil {
			jsonErr(w, "invalid inheritedRoles JSON: "+err.Error(), 400)
			return
		}
	}
	
	// Build createRole command
	cmd := bson.D{
		{Key: "createRole", Value: roleName},
		{Key: "privileges", Value: privileges},
		{Key: "roles", Value: inheritedRoles},
	}
	
	var result bson.M
	err = c.Database(dbName).RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	
	jsonOK(w, map[string]string{"ok": "1", "message": "Role " + roleName + " created"})
}

func handleUpdateUserRoles(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	username := strings.TrimSpace(r.FormValue("username"))
	dbName := strings.TrimSpace(r.FormValue("db"))
	rolesJSON := r.FormValue("roles") // JSON string: [{role: "name", db: "dbname"}]
	
	if username == "" || dbName == "" {
		jsonErr(w, "username and db required", 400)
		return
	}
	
	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	
	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	// Parse roles from JSON
	var roles []interface{}
	if rolesJSON != "" {
		if err := json.Unmarshal([]byte(rolesJSON), &roles); err != nil {
			jsonErr(w, "invalid roles JSON: "+err.Error(), 400)
			return
		}
	}
	
	// Build grantRolesToUser command
	cmd := bson.D{
		{Key: "grantRolesToUser", Value: username},
		{Key: "roles", Value: roles},
	}
	
	var result bson.M
	err = c.Database(dbName).RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	
	jsonOK(w, map[string]string{"ok": "1", "message": "Roles updated for user " + username})
}

// handleGetRoleDetail fetches full privileges for one role
func handleGetRoleDetail(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	roleName := strings.TrimSpace(r.FormValue("roleName"))

	if dbName == "" || roleName == "" {
		jsonErr(w, "db and roleName required", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var result bson.D
	err = c.Database(dbName).RunCommand(ctx, bson.D{
		{Key: "rolesInfo", Value: bson.A{bson.D{{Key: "role", Value: roleName}, {Key: "db", Value: dbName}}}},
		{Key: "showPrivileges", Value: true},
		{Key: "showBuiltinRoles", Value: false},
	}).Decode(&result)

	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}

	resultMap := bsonToAny(result).(map[string]interface{})
	rolesList := toSlice(resultMap["roles"])
	if len(rolesList) == 0 {
		jsonErr(w, "role not found", 404)
		return
	}
	jsonOK(w, rolesList[0])
}

func handleDeleteRole(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	roleName := strings.TrimSpace(r.FormValue("roleName"))
	dbName := strings.TrimSpace(r.FormValue("db"))
	
	if roleName == "" || dbName == "" {
		jsonErr(w, "roleName and db required", 400)
		return
	}
	
	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	
	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	cmd := bson.D{{Key: "dropRole", Value: roleName}}
	
	var result bson.M
	err = c.Database(dbName).RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	
	jsonOK(w, map[string]string{"ok": "1", "message": "Role " + roleName + " deleted"})
}

func handleCreateUser(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	username  := strings.TrimSpace(r.FormValue("username"))
	password  := strings.TrimSpace(r.FormValue("password"))
	dbName    := strings.TrimSpace(r.FormValue("db"))
	rolesJSON := r.FormValue("roles") // [{role:"...",db:"..."}]

	if username == "" || password == "" || dbName == "" {
		jsonErr(w, "username, password and db required", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var roles []interface{}
	if rolesJSON != "" {
		if err := json.Unmarshal([]byte(rolesJSON), &roles); err != nil {
			jsonErr(w, "invalid roles JSON: "+err.Error(), 400)
			return
		}
	}
	if roles == nil {
		roles = []interface{}{}
	}

	cmd := bson.D{
		{Key: "createUser", Value: username},
		{Key: "pwd",        Value: password},
		{Key: "roles",      Value: roles},
	}

	var result bson.M
	err = c.Database(dbName).RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	jsonOK(w, map[string]string{"ok": "1", "message": "User " + username + " created"})
}

func handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri      := formURI(r)
	username := strings.TrimSpace(r.FormValue("username"))
	dbName   := strings.TrimSpace(r.FormValue("db"))

	if username == "" || dbName == "" {
		jsonErr(w, "username and db required", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var result bson.M
	err = c.Database(dbName).RunCommand(ctx,
		bson.D{{Key: "dropUser", Value: username}},
	).Decode(&result)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	jsonOK(w, map[string]string{"ok": "1", "message": "User " + username + " deleted"})
}

// handleSetUserRoles replaces all roles on a user (revokeAll + grant).
// MongoDB 6.0+ updateUser command handles this atomically.
func handleSetUserRoles(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri      := formURI(r)
	username := strings.TrimSpace(r.FormValue("username"))
	dbName   := strings.TrimSpace(r.FormValue("db"))
	rolesJSON := r.FormValue("roles")

	if username == "" || dbName == "" {
		jsonErr(w, "username and db required", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var roles []interface{}
	if rolesJSON != "" {
		if err := json.Unmarshal([]byte(rolesJSON), &roles); err != nil {
			jsonErr(w, "invalid roles JSON: "+err.Error(), 400)
			return
		}
	}
	if roles == nil {
		roles = []interface{}{}
	}

	cmd := bson.D{
		{Key: "updateUser", Value: username},
		{Key: "roles",      Value: roles},
	}

	var result bson.M
	err = c.Database(dbName).RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	jsonOK(w, map[string]string{"ok": "1", "message": "Roles updated for " + username})
}

// ─── Cross-shard user/role sync ───────────────────────────────────────────
//
// In a sharded cluster, cluster-wide users live on the config servers and are
// federated via mongos, but each shard also has its own *local* user/role
// store (used e.g. by replication and shard-local applications). When a DBA
// creates a user/role on one shard's primary, the others don't see it. These
// handlers help bring missing entries across.

// formTargetURIs reads the multi-valued `targetUri` form field and applies
// per-shard credentials to each URI. The frontend sends one form value per
// target — commas can't be used as a separator because they appear inside
// replica-set URIs (multi-host strings).
func formTargetURIs(r *http.Request, creds map[string]struct{ username, password string }) []string {
	raw := r.Form["targetUri"]
	out := make([]string, 0, len(raw))
	for _, t := range raw {
		t = strings.TrimSpace(t)
		if t == "" { continue }
		out = append(out, applyShardCredentials(t, creds))
	}
	return out
}

// handleCheckPresence — POST /api/user/check-presence
// Inputs: kind=user|role, name, db, targetUris (comma-separated)
// Output: [{shardId, uri (masked), exists, error}]
//
// Used by the sync modal to populate the "missing on these shards" list
// before the DBA confirms the push. Tolerates per-target errors: an
// unreachable shard reports the error string rather than failing the call.
func handleCheckPresence(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	kind   := strings.TrimSpace(r.FormValue("kind"))
	name   := strings.TrimSpace(r.FormValue("name"))
	dbName := strings.TrimSpace(r.FormValue("db"))
	if (kind != "user" && kind != "role") || name == "" || dbName == "" {
		jsonErr(w, "kind (user|role), name, db, targetUri[] required", 400)
		return
	}
	shardCreds := parseShardCredentials(r)
	targets := formTargetURIs(r, shardCreds)
	if len(targets) == 0 {
		jsonErr(w, "at least one targetUri required", 400)
		return
	}

	type result struct {
		ShardId string `json:"shardId"`
		Uri     string `json:"uri"`
		Exists  bool   `json:"exists"`
		Error   string `json:"error,omitempty"`
	}
	results := make([]result, 0, len(targets))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, t := range targets {
		res := result{ShardId: extractShardID(t), Uri: maskURI(t)}
		c, err := getClient(t)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		var cmd bson.D
		if kind == "user" {
			cmd = bson.D{{Key: "usersInfo", Value: bson.D{
				{Key: "user", Value: name}, {Key: "db", Value: dbName},
			}}}
		} else {
			cmd = bson.D{{Key: "rolesInfo", Value: bson.A{bson.D{
				{Key: "role", Value: name}, {Key: "db", Value: dbName},
			}}}}
		}
		var raw bson.D
		if err := c.Database(dbName).RunCommand(ctx, cmd).Decode(&raw); err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		rm := bsonToAny(raw).(map[string]interface{})
		key := "users"
		if kind == "role" { key = "roles" }
		if arr, ok := rm[key].([]interface{}); ok && len(arr) > 0 {
			res.Exists = true
		}
		results = append(results, res)
	}
	jsonOK(w, results)
}

// handleSyncUser — POST /api/user/sync-user
// Inputs: uri (source), username, password, db, targetUris (comma-separated)
// Reads the user's role list from the source and creates the same user on
// handleSyncUser — POST /api/user/sync-user
// Inputs: uri (source), username, password (mode=plaintext only), db,
//         targetUri[] (multi-valued), mode (optional: "plaintext" | "clone")
//
// Two modes:
//
//   plaintext (default) — the supported path. Reads the user's role list
//     from `usersInfo` on the source and runs `createUser` on every target
//     with the DBA-supplied plaintext password. Each shard ends up with its
//     own SCRAM salt and hash for the same plaintext — perfectly fine for
//     authentication; just not byte-identical credentials.
//
//   clone — the back-door path. Reads the full user document from the
//     source (with showCredentials:true so the SCRAM-SHA-1/256 credentials,
//     including iterationCount, salt, storedKey, and serverKey, are
//     returned), then drives `_mergeAuthzCollections` on each target via a
//     temporary collection (`admin.tempusers`). This is the same internal
//     command mongorestore uses to import users with their hashes intact.
//     Inserting into admin.system.users directly is *not* allowed, so the
//     temp-collection + merge dance is the only way.
//
//   Privilege requirement for clone:
//     The MongoAdmin session must have the built-in `restore` role on the
//     target's admin database (or `__system`). `userAdminAnyDatabase` alone
//     is not enough — `_mergeAuthzCollections` is restricted to backup/
//     restore service accounts because it can overwrite *any* user or role
//     on the target.
//
//   Security note for clone:
//     Cloning bypasses createUser's password-strength enforcement (none
//     today, but a guard nevertheless) and copies whatever auth state the
//     source has — including SCRAM-SHA-1-only users that the rest of the
//     deployment may have disabled. Use with care.
func handleSyncUser(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	srcUri   := formURI(r)
	username := strings.TrimSpace(r.FormValue("username"))
	password := r.FormValue("password")
	dbName   := strings.TrimSpace(r.FormValue("db"))
	mode     := strings.TrimSpace(r.FormValue("mode"))
	if mode == "" { mode = "plaintext" }
	if mode != "plaintext" && mode != "clone" {
		jsonErr(w, "mode must be 'plaintext' or 'clone'", 400)
		return
	}
	if username == "" || dbName == "" {
		jsonErr(w, "username, db, targetUri[] required", 400)
		return
	}
	if mode == "plaintext" && password == "" {
		jsonErr(w, "password required in plaintext mode", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	srcUri = applyShardCredentials(srcUri, shardCreds)
	targets := formTargetURIs(r, shardCreds)
	if len(targets) == 0 {
		jsonErr(w, "at least one targetUri required", 400)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srcClient, err := getClient(srcUri)
	if err != nil { jsonErr(w, "source: "+err.Error(), 500); return }

	type result struct {
		ShardId string `json:"shardId"`
		Uri     string `json:"uri"`
		Ok      bool   `json:"ok"`
		Updated bool   `json:"updated,omitempty"` // plaintext mode: updateUser ran instead of createUser
		Skipped bool   `json:"skipped,omitempty"`
		Error   string `json:"error,omitempty"`
	}
	results := make([]result, 0, len(targets))

	if mode == "plaintext" {
		updateIfExists := r.FormValue("updateIfExists") == "1"
		// Standard path: copy role list only, re-hash a fresh password.
		var rawSrc bson.D
		err = srcClient.Database(dbName).RunCommand(ctx, bson.D{
			{Key: "usersInfo", Value: bson.D{{Key: "user", Value: username}, {Key: "db", Value: dbName}}},
		}).Decode(&rawSrc)
		if err != nil { jsonErr(w, "fetch user: "+err.Error(), 500); return }

		srcMap := bsonToAny(rawSrc).(map[string]interface{})
		usersArr, ok := srcMap["users"].([]interface{})
		if !ok || len(usersArr) == 0 {
			jsonErr(w, "user '"+username+"' not found on source", 404)
			return
		}
		roles := usersArr[0].(map[string]interface{})["roles"]
		if roles == nil { roles = []interface{}{} }

		for _, t := range targets {
			res := result{ShardId: extractShardID(t), Uri: maskURI(t)}
			c, err := getClient(t)
			if err != nil {
				res.Error = err.Error()
				results = append(results, res)
				continue
			}
			cmd := bson.D{
				{Key: "createUser", Value: username},
				{Key: "pwd",        Value: password},
				{Key: "roles",      Value: roles},
			}
			var raw bson.D
			if err := c.Database(dbName).RunCommand(ctx, cmd).Decode(&raw); err != nil {
				if strings.Contains(err.Error(), "already exists") {
					if updateIfExists {
						// User exists; overwrite role list (and password) to
						// match the source. This is the "drift-repair" path —
						// the DBA explicitly opted in via the modal checkbox.
						updateCmd := bson.D{
							{Key: "updateUser", Value: username},
							{Key: "roles",      Value: roles},
							{Key: "pwd",        Value: password},
						}
						var uRaw bson.D
						if uerr := c.Database(dbName).RunCommand(ctx, updateCmd).Decode(&uRaw); uerr != nil {
							res.Error = "update failed: " + uerr.Error()
						} else {
							res.Ok      = true
							res.Updated = true
						}
					} else {
						res.Skipped = true
					}
				} else {
					res.Error = err.Error()
				}
			} else {
				res.Ok = true
			}
			results = append(results, res)
		}
		jsonOK(w, results)
		return
	}

	// ── clone mode: _mergeAuthzCollections via temp collection ───────────
	// 1. Fetch the full user doc *with credentials* from the source. The
	//    response from usersInfo with showCredentials:true is at users[0]
	//    and contains _id, user, db, credentials, roles, etc. We pass that
	//    same shape into admin.tempusers on each target.
	var rawSrc bson.D
	err = srcClient.Database(dbName).RunCommand(ctx, bson.D{
		{Key: "usersInfo",       Value: bson.D{{Key: "user", Value: username}, {Key: "db", Value: dbName}}},
		{Key: "showCredentials", Value: true},
	}).Decode(&rawSrc)
	if err != nil { jsonErr(w, "fetch user with credentials: "+err.Error(), 500); return }

	srcMap := bsonToAny(rawSrc).(map[string]interface{})
	usersArr, ok := srcMap["users"].([]interface{})
	if !ok || len(usersArr) == 0 {
		jsonErr(w, "user '"+username+"' not found on source", 404)
		return
	}
	userDoc, _ := usersArr[0].(map[string]interface{})
	if userDoc == nil || userDoc["credentials"] == nil {
		jsonErr(w, "source returned no credentials — does the connected user have viewUser or showCredentials permission?", 403)
		return
	}

	// Reshape the user document for the merge. _mergeAuthzCollections expects
	// a doc that looks like admin.system.users: keys _id, user, db, credentials,
	// roles, and optional customData / authenticationRestrictions. We drop
	// userId (server regenerates it) and any other transient fields.
	mergeDoc := bson.M{
		"_id":         userDoc["_id"],
		"user":        userDoc["user"],
		"db":          userDoc["db"],
		"credentials": userDoc["credentials"],
		"roles":       userDoc["roles"],
	}
	// Sometimes _id isn't returned by usersInfo; reconstruct it as "<db>.<user>".
	if mergeDoc["_id"] == nil {
		mergeDoc["_id"] = fmt.Sprintf("%s.%s", dbName, username)
	}
	if cd, exists := userDoc["customData"]; exists && cd != nil {
		mergeDoc["customData"] = cd
	}
	if ar, exists := userDoc["authenticationRestrictions"]; exists && ar != nil {
		mergeDoc["authenticationRestrictions"] = ar
	}

	// Use a unique temp collection per call to avoid collisions if two DBAs
	// run sync simultaneously. mongorestore uses "admin.tempusers" as a
	// well-known name, but it doesn't tolerate concurrent runs — we generate
	// a per-request suffix so concurrent syncs don't trample each other.
	suffix        := time.Now().UnixNano()
	tempUsersColl := fmt.Sprintf("tempusers_%d", suffix)
	tempRolesColl := fmt.Sprintf("temproles_%d", suffix)

	for _, t := range targets {
		res := result{ShardId: extractShardID(t), Uri: maskURI(t)}
		c, err := getClient(t)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		adminDb   := c.Database("admin")
		usersColl := adminDb.Collection(tempUsersColl)
		rolesColl := adminDb.Collection(tempRolesColl)

		// Best-effort cleanup of any stale collections from a previous failed
		// attempt — ignore errors since they usually won't exist.
		_ = usersColl.Drop(ctx)
		_ = rolesColl.Drop(ctx)

		if _, ierr := usersColl.InsertOne(ctx, mergeDoc); ierr != nil {
			res.Error = "insert temp user doc: " + ierr.Error()
			results = append(results, res)
			continue
		}
		// Modern MongoDB requires `tempRolesCollection` to be set even when
		// we're only merging users (the IDL parser is strict about it). Pass
		// an empty collection so the role-merge portion is a no-op.
		// See mongo-tools JIRA TOOLS-2946 for the historical rationale.
		_ = adminDb.CreateCollection(ctx, tempRolesColl)

		// drop:false → merge with existing users (doesn't wipe other accounts).
		// drop:true would replace the entire authoritative user set on the
		// target — never what an interactive sync wants.
		// `db` scopes the merge to user docs whose stored `db` field matches.
		// MongoDB 7.0+ rejects the command without it (IDLFailedToParse).
		// See https://pkg.go.dev/github.com/mongodb/mongo-tools/mongorestore.
		mergeCmd := bson.D{
			{Key: "_mergeAuthzCollections", Value: 1},
			{Key: "db",                     Value: dbName},
			{Key: "tempUsersCollection",    Value: "admin." + tempUsersColl},
			{Key: "tempRolesCollection",    Value: "admin." + tempRolesColl},
			{Key: "drop",                   Value: false},
			{Key: "writeConcern",           Value: bson.D{{Key: "w", Value: "majority"}}},
		}
		var raw bson.D
		merr := adminDb.RunCommand(ctx, mergeCmd).Decode(&raw)
		// Always drop both temp collections, regardless of merge result.
		_ = usersColl.Drop(ctx)
		_ = rolesColl.Drop(ctx)

		if merr != nil {
			if strings.Contains(merr.Error(), "already exists") ||
				strings.Contains(merr.Error(), "duplicate key") {
				res.Skipped = true
			} else {
				res.Error = merr.Error()
			}
		} else {
			res.Ok = true
		}
		results = append(results, res)
	}
	jsonOK(w, results)
}

// handleSyncRole — POST /api/user/sync-role
// Inputs: uri (source), roleName, db, targetUris (comma-separated)
// Reads the role's privileges + inheritedRoles from the source and creates
// it on every target. Unlike users, no password is needed.
func handleSyncRole(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	srcUri   := formURI(r)
	roleName := strings.TrimSpace(r.FormValue("roleName"))
	dbName   := strings.TrimSpace(r.FormValue("db"))
	if roleName == "" || dbName == "" {
		jsonErr(w, "roleName, db, targetUri[] required", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	srcUri = applyShardCredentials(srcUri, shardCreds)
	targets := formTargetURIs(r, shardCreds)
	if len(targets) == 0 {
		jsonErr(w, "at least one targetUri required", 400)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Fetch source role definition with privileges.
	srcClient, err := getClient(srcUri)
	if err != nil { jsonErr(w, "source: "+err.Error(), 500); return }
	var rawSrc bson.D
	err = srcClient.Database(dbName).RunCommand(ctx, bson.D{
		{Key: "rolesInfo", Value: bson.A{bson.D{{Key: "role", Value: roleName}, {Key: "db", Value: dbName}}}},
		{Key: "showPrivileges", Value: true},
	}).Decode(&rawSrc)
	if err != nil { jsonErr(w, "fetch role: "+err.Error(), 500); return }

	srcMap := bsonToAny(rawSrc).(map[string]interface{})
	rolesArr, ok := srcMap["roles"].([]interface{})
	if !ok || len(rolesArr) == 0 {
		jsonErr(w, "role '"+roleName+"' not found on source", 404)
		return
	}
	role := rolesArr[0].(map[string]interface{})
	if b, _ := role["isBuiltin"].(bool); b {
		jsonErr(w, "cannot sync built-in roles", 400)
		return
	}
	privileges := role["privileges"]
	if privileges == nil { privileges = []interface{}{} }
	inheritedRoles := role["inheritedRoles"]
	if inheritedRoles == nil { inheritedRoles = []interface{}{} }

	type result struct {
		ShardId string `json:"shardId"`
		Uri     string `json:"uri"`
		Ok      bool   `json:"ok"`
		Skipped bool   `json:"skipped,omitempty"`
		Error   string `json:"error,omitempty"`
	}
	results := make([]result, 0, len(targets))

	for _, t := range targets {
		res := result{ShardId: extractShardID(t), Uri: maskURI(t)}
		c, err := getClient(t)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		cmd := bson.D{
			{Key: "createRole", Value: roleName},
			{Key: "privileges", Value: privileges},
			{Key: "roles",      Value: inheritedRoles},
		}
		var raw bson.D
		if err := c.Database(dbName).RunCommand(ctx, cmd).Decode(&raw); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				res.Skipped = true
			} else {
				res.Error = err.Error()
			}
		} else {
			res.Ok = true
		}
		results = append(results, res)
	}
	jsonOK(w, results)
}

// handleUpdateRole replaces inherited roles + privileges on a custom role.
func handleUpdateRole(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri            := formURI(r)
	roleName       := strings.TrimSpace(r.FormValue("roleName"))
	dbName         := strings.TrimSpace(r.FormValue("db"))
	privilegesJSON := r.FormValue("privileges")
	inheritedJSON  := r.FormValue("inheritedRoles")

	if roleName == "" || dbName == "" {
		jsonErr(w, "roleName and db required", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var privileges []interface{}
	if privilegesJSON != "" {
		if err := json.Unmarshal([]byte(privilegesJSON), &privileges); err != nil {
			jsonErr(w, "invalid privileges JSON: "+err.Error(), 400)
			return
		}
	}
	if privileges == nil {
		privileges = []interface{}{}
	}

	var inheritedRoles []interface{}
	if inheritedJSON != "" {
		if err := json.Unmarshal([]byte(inheritedJSON), &inheritedRoles); err != nil {
			jsonErr(w, "invalid inheritedRoles JSON: "+err.Error(), 400)
			return
		}
	}
	if inheritedRoles == nil {
		inheritedRoles = []interface{}{}
	}

	cmd := bson.D{
		{Key: "updateRole",  Value: roleName},
		{Key: "privileges",  Value: privileges},
		{Key: "roles",       Value: inheritedRoles},
	}

	var result bson.M
	err = c.Database(dbName).RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") {
			shardID := extractShardID(uri)
			jsonErrWithShard(w, err.Error(), shardID, uri, 401)
		} else {
			jsonErr(w, err.Error(), 500)
		}
		return
	}
	jsonOK(w, map[string]string{"ok": "1", "message": "Role " + roleName + " updated"})
}

// ── Kill Op ───────────────────────────────────────────────────────────────────

// handleKillOp kills one or more operations.
//
// Single kill:  POST /api/kill-op  uri=... opid=<number>
// Batch kill:   POST /api/kill-op  uri=... batch=true  [filter params below]
//   op=<find|command|...>   — optional op type filter
//   ns=<db.coll>            — optional namespace filter
//   min_secs=<N>            — only kill if running >= N seconds (default 0)
//   app=<appName>           — optional app name filter
//
// Returns { killed: N, errors: [...] }
func handleKillOp(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Single kill ──────────────────────────────────────────────────────────
	if r.FormValue("batch") != "true" {
		opidStr := strings.TrimSpace(r.FormValue("opid"))
		if opidStr == "" {
			jsonErr(w, "opid required", 400)
			return
		}
		var opid interface{}
		var n int64
		if _, e := fmt.Sscanf(opidStr, "%d", &n); e == nil {
			opid = n
		} else {
			opid = opidStr // string opid for mongos
		}
		err = c.Database("admin").RunCommand(ctx, bson.D{{Key: "killOp", Value: 1}, {Key: "op", Value: opid}}).Err()
		if err != nil {
			jsonErr(w, err.Error(), 500)
			return
		}
		jsonOK(w, map[string]interface{}{"killed": 1, "opid": opidStr})
		return
	}

	// ── Batch kill ───────────────────────────────────────────────────────────
	// First fetch current ops
	pipeline := bson.A{
		bson.D{{Key: "$currentOp", Value: bson.D{
			{Key: "allUsers", Value: true},
			{Key: "idleConnections", Value: false},
			{Key: "localOps", Value: true},
		}}},
	}
	cur, err := c.Database("admin").Aggregate(ctx, pipeline)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	defer cur.Close(ctx)

	var rawOps []bson.D
	if err := cur.All(ctx, &rawOps); err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}

	// Parse filter params
	filterOp     := strings.TrimSpace(r.FormValue("op"))
	filterNs     := strings.TrimSpace(r.FormValue("ns"))
	filterApp    := strings.TrimSpace(r.FormValue("app"))
	minSecsStr   := strings.TrimSpace(r.FormValue("min_secs"))
	minSecs      := int64(0)
	if minSecsStr != "" {
		fmt.Sscanf(minSecsStr, "%d", &minSecs)
	}

	debugLog(fmt.Sprintf("handleKillOp batch: op=%q ns=%q app=%q min_secs=%d", filterOp, filterNs, filterApp, minSecs))

	killed := 0
	var errors []string

	for _, rawOp := range rawOps {
		op := bsonToAny(rawOp).(map[string]interface{})

		// Apply filters
		opType, _ := op["op"].(string)
		ns, _     := op["ns"].(string)
		appName   := ""
		if cm, ok := op["clientMetadata"].(map[string]interface{}); ok {
			if app, ok := cm["application"].(map[string]interface{}); ok {
				appName, _ = app["name"].(string)
			}
		}
		secs := toInt64(op["secs_running"])

		if filterOp  != "" && !strings.EqualFold(opType, filterOp) { continue }
		if filterNs  != "" && !strings.Contains(ns, filterNs)      { continue }
		if filterApp != "" && !strings.Contains(appName, filterApp) { continue }
		if secs < minSecs                                           { continue }

		// Get opid
		var opid interface{}
		switch v := op["opid"].(type) {
		case int32:  opid = int64(v)
		case int64:  opid = v
		case float64:opid = int64(v)
		case string: opid = v
		default:     continue
		}

		debugLog(fmt.Sprintf("handleKillOp: killing op %v (type=%s ns=%s secs=%d)", opid, opType, ns, secs))

		killErr := c.Database("admin").RunCommand(ctx,
			bson.D{{Key: "killOp", Value: 1}, {Key: "op", Value: opid}},
		).Err()
		if killErr != nil {
			errors = append(errors, fmt.Sprintf("opid %v: %s", opid, killErr.Error()))
		} else {
			killed++
		}
	}

	jsonOK(w, map[string]interface{}{"killed": killed, "errors": errors})
}

// ── Collection Stats ──────────────────────────────────────────────────────────

type CollStat struct {
	Name         string  `json:"name"`
	Count        int64   `json:"count"`
	DataSize     int64   `json:"dataSize"`
	StorageSize  int64   `json:"storageSize"`
	TotalIndexSize int64 `json:"totalIndexSize"`
	AvgObjSize   float64 `json:"avgObjSize"`
	NumIndexes   int64   `json:"numIndexes"`
	IsSharded    bool    `json:"isSharded"`
	ShardKey     string  `json:"shardKey,omitempty"`
}

func handleCollStats(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri    := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	if dbName == "" { jsonErr(w, "db required", 400); return }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	names, err := c.Database(dbName).ListCollectionNames(ctx, bson.D{})
	if err != nil { jsonErr(w, err.Error(), 500); return }

	// Get sharded collections from config
	shardedColls := map[string]string{} // ns -> shard key
	if scRes, err2 := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "collections"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + dbName + "\\."}}},
		}},
		{Key: "limit", Value: 1000},
	}); err2 == nil {
		if cursor := toMap(scRes["cursor"]); cursor != nil {
			for _, row := range toSlice(cursor["firstBatch"]) {
				if m := toMap(row); m != nil {
					ns, _ := m["_id"].(string)
					keyStr := ""
					if kv := m["key"]; kv != nil {
						if km, ok := kv.(map[string]interface{}); ok {
							parts := []string{}
							for k := range km { parts = append(parts, k) }
							sort.Strings(parts)
							keyStr = strings.Join(parts, ", ")
						}
					}
					shardedColls[ns] = keyStr
				}
			}
		}
	}

	var mu sync.Mutex
	var stats []CollStat
	var wg sync.WaitGroup

	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			var raw bson.D
			err2 := c.Database(dbName).RunCommand(ctx, bson.D{
				{Key: "collStats", Value: name},
				{Key: "scale", Value: 1},
			}).Decode(&raw)
			if err2 != nil { return }
			m := bsonToAny(raw).(map[string]interface{})
			ns := dbName + "." + name
			sk, isSharded := shardedColls[ns]
			cs := CollStat{
				Name:           name,
				Count:          toInt64(m["count"]),
				DataSize:       toInt64(m["size"]),
				StorageSize:    toInt64(m["storageSize"]),
				TotalIndexSize: toInt64(m["totalIndexSize"]),
				NumIndexes:     toInt64(m["nindexes"]),
				IsSharded:      isSharded,
				ShardKey:       sk,
			}
			if v, ok := m["avgObjSize"].(float64); ok { cs.AvgObjSize = v }
			mu.Lock()
			stats = append(stats, cs)
			mu.Unlock()
		}(name)
	}
	wg.Wait()
	sort.Slice(stats, func(i, j int) bool { return stats[i].DataSize > stats[j].DataSize })
	if stats == nil { stats = []CollStat{} }
	jsonOK(w, stats)
}

// ── Index Management ──────────────────────────────────────────────────────────

type IndexInfo struct {
	Name         string                 `json:"name"`
	Keys         map[string]interface{} `json:"keys"`
	Unique       bool                   `json:"unique"`
	Sparse       bool                   `json:"sparse"`
	TTL          int64                  `json:"ttl"`
	Partial      bool                   `json:"partial"`
	Hidden       bool                   `json:"hidden"`
	Accesses     int64                  `json:"accesses"`
	Since        string                 `json:"since"`
	SizeBytes    int64                  `json:"sizeBytes"`
	ShardKeyIdx  bool                   `json:"shardKeyIdx"` // true = sole index covering shard key
}

func handleListIndexes(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri    := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	coll   := strings.TrimSpace(r.FormValue("collection"))
	if dbName == "" || coll == "" { jsonErr(w, "db and collection required", 400); return }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// listIndexes
	var listRes bson.D
	err = c.Database(dbName).RunCommand(ctx, bson.D{{Key: "listIndexes", Value: coll}}).Decode(&listRes)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	listMap := bsonToAny(listRes).(map[string]interface{})

	// $indexStats
	usageMap := map[string]int64{}
	sinceMap := map[string]string{}
	if cursor, err2 := c.Database(dbName).Collection(coll).Aggregate(ctx,
		bson.A{bson.D{{Key: "$indexStats", Value: bson.D{}}}},
	); err2 == nil {
		defer cursor.Close(ctx)
		var rows []bson.D
		cursor.All(ctx, &rows)
		for _, row := range rows {
			m := bsonToAny(row).(map[string]interface{})
			name, _ := m["name"].(string)
			if acc := toMap(m["accesses"]); acc != nil {
				usageMap[name] = toInt64(acc["ops"])
				sinceMap[name], _ = acc["since"].(string)
			}
		}
	}

	// Index sizes from collStats
	sizeMap := map[string]int64{}
	var statsRes bson.D
	if err2 := c.Database(dbName).RunCommand(ctx, bson.D{
		{Key: "collStats", Value: coll}, {Key: "scale", Value: 1},
	}).Decode(&statsRes); err2 == nil {
		sm := bsonToAny(statsRes).(map[string]interface{})
		if isz := toMap(sm["indexSizes"]); isz != nil {
			for k, v := range isz { sizeMap[k] = toInt64(v) }
		}
	}

	var indexes []IndexInfo
	// Fetch shard key from config.collections (sharded only; harmless on plain RS)
	shardKeyFields := map[string]bool{}
	ns := dbName + "." + coll
	if skRes, err3 := runCmd(uri, "config", bson.D{
		{Key: "find", Value: "collections"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: ns}}},
	}); err3 == nil {
		if cur := toMap(skRes["cursor"]); cur != nil {
			for _, row := range toSlice(cur["firstBatch"]) {
				if m := toMap(row); m != nil {
					if kv := m["key"]; kv != nil {
						if km, ok := kv.(map[string]interface{}); ok {
							for k := range km { shardKeyFields[k] = true }
						}
					}
				}
			}
		}
	}
	// An index "covers" the shard key if its key prefix contains all shard key fields.
	coversShardKey := func(idxKeys map[string]interface{}) bool {
		if len(shardKeyFields) == 0 { return false }
		for sk := range shardKeyFields {
			if _, ok := idxKeys[sk]; !ok { return false }
		}
		return true
	}
	// Count how many visible (non-hidden) indexes cover the shard key
	// We mark an index as shardKeyIdx only when it is the sole such index.
	if cursor := toMap(listMap["cursor"]); cursor != nil {
		for _, row := range toSlice(cursor["firstBatch"]) {
			m, ok := row.(map[string]interface{})
			if !ok { continue }
			name, _ := m["name"].(string)
			keys, _ := m["key"].(map[string]interface{})
			ttl := int64(-1)
			if v, ok := m["expireAfterSeconds"]; ok { ttl = toInt64(v) }
			unique, _ := m["unique"].(bool)
			sparse, _ := m["sparse"].(bool)
			hidden, _ := m["hidden"].(bool)
			partial := m["partialFilterExpression"] != nil
			indexes = append(indexes, IndexInfo{
				Name: name, Keys: keys,
				Unique: unique, Sparse: sparse, Hidden: hidden, Partial: partial,
				TTL:       ttl,
				Accesses:  usageMap[name],
				Since:     sinceMap[name],
				SizeBytes: sizeMap[name],
				ShardKeyIdx: coversShardKey(keys),
			})
		}
	}
	// If more than one non-hidden index covers the shard key, none is "the sole" one
	// and dropping any of them is allowed. Only lock when count == 1.
	coverCount := 0
	for _, idx := range indexes {
		if idx.ShardKeyIdx && !idx.Hidden { coverCount++ }
	}
	if coverCount != 1 {
		for i := range indexes { indexes[i].ShardKeyIdx = false }
	}
	if indexes == nil { indexes = []IndexInfo{} }
	jsonOK(w, indexes)
}

func handleDropIndex(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri    := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	coll   := strings.TrimSpace(r.FormValue("collection"))
	name   := strings.TrimSpace(r.FormValue("name"))
	if dbName == "" || coll == "" || name == "" { jsonErr(w, "db, collection and name required", 400); return }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var res bson.D
	err = c.Database(dbName).RunCommand(ctx, bson.D{
		{Key: "dropIndexes", Value: coll},
		{Key: "index", Value: name},
	}).Decode(&res)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	jsonOK(w, map[string]string{"ok": "1", "message": "Dropped index " + name})
}

// ── Oplog Stats ───────────────────────────────────────────────────────────────

func handleOplogStats(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	rsURIs := r.Form["rs_uri"]
	if len(rsURIs) == 0 { rsURIs = []string{formURI(r)} }

	type MemberLag struct {
		Host       string  `json:"host"`
		State      string  `json:"state"`
		LagSecs    float64 `json:"lagSecs"`
		OptimeTS   int64   `json:"optimeTS"`   // applied optime — seconds component of the BSON timestamp
		OptimeDate string  `json:"optimeDate"` // applied optime — wall-clock date (RFC3339)
	}
	type RSOplog struct {
		SetName     string      `json:"setName"`
		URI         string      `json:"uri"`
		SizeMB      int64       `json:"sizeMB"`
		UsedMB      int64       `json:"usedMB"`
		UsedPct     float64     `json:"usedPct"`
		WindowHours float64     `json:"windowHours"`
		FirstTS     int64       `json:"firstTS"`     // oldest oplog entry — unix seconds (UTC)
		LastTS      int64       `json:"lastTS"`      // newest oplog entry — unix seconds (UTC)
		Count       int64       `json:"count"`       // oplog.rs document count
		AvgObjSize  float64     `json:"avgObjSize"`  // average oplog entry size, bytes
		WriteRateMB float64     `json:"writeRateMB"` // average oplog write rate, MB/hour
		EstRetHours float64     `json:"estRetHours"` // estimated retention once the oplog is full, hours
		Members     []MemberLag `json:"members"`
		Error       string      `json:"error,omitempty"`
	}

	var mu sync.Mutex
	var results []RSOplog
	var wg sync.WaitGroup

	for _, rsURI := range rsURIs {
		wg.Add(1)
		go func(rsURI string) {
			defer wg.Done()
			result := RSOplog{URI: rsURI}

			c, err := getClient(rsURI)
			if err != nil { result.Error = err.Error(); mu.Lock(); results = append(results, result); mu.Unlock(); return }
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			// RS name from replSetGetStatus
			var statusD bson.D
			if err2 := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&statusD); err2 == nil {
				sm := bsonToAny(statusD).(map[string]interface{})
				result.SetName, _ = sm["set"].(string)

				// Primary optime for lag calc
				var primaryOptime float64
				for _, mem := range toSlice(sm["members"]) {
					mm := toMap(mem)
					if mm == nil { continue }
					if state, _ := mm["stateStr"].(string); state == "PRIMARY" {
						if ot := toMap(mm["optime"]); ot != nil {
							if ts, ok := ot["ts"].(string); ok { _ = ts } // already string from bsonToAny
						}
						// Use optimeDate which bsonToAny converts to RFC3339 string
						if od, ok := mm["optimeDate"].(string); ok {
							if t, err3 := time.Parse(time.RFC3339, od); err3 == nil {
								primaryOptime = float64(t.Unix())
							}
						}
					}
				}
				for _, mem := range toSlice(sm["members"]) {
					mm := toMap(mem)
					if mm == nil { continue }
					host, _ := mm["name"].(string)
					state, _ := mm["stateStr"].(string)
					lag := 0.0
					if primaryOptime > 0 {
						if od, ok := mm["optimeDate"].(string); ok {
							if t, err3 := time.Parse(time.RFC3339, od); err3 == nil {
								lag = primaryOptime - float64(t.Unix())
								if lag < 0 { lag = 0 }
							}
						}
					}
					ml := MemberLag{Host: host, State: state, LagSecs: lag}
					// replSetGetStatus → members[].optime is the applied optime for
					// that member (what the connected node reports as optimes.appliedOpTime
					// for itself). The seconds component shows exactly where each
					// secondary has caught up to in the oplog stream.
					if ot := toMap(mm["optime"]); ot != nil {
						ml.OptimeTS = tsSecondsFromAny(ot["ts"])
					}
					if od, ok := mm["optimeDate"].(string); ok {
						ml.OptimeDate = od
					}
					result.Members = append(result.Members, ml)
				}
			}

			// Oplog collection stats. We request scale=1 (bytes) and convert to
			// MB ourselves: a MB scale would also divide avgObjSize, rendering it
			// useless, and count must stay as a raw document count.
			var raw bson.D
			if err2 := c.Database("local").RunCommand(ctx, bson.D{
				{Key: "collStats", Value: "oplog.rs"},
				{Key: "scale", Value: 1},
			}).Decode(&raw); err2 == nil {
				m := bsonToAny(raw).(map[string]interface{})
				const mb = 1024 * 1024
				result.SizeMB     = toInt64(m["maxSize"]) / mb
				result.UsedMB     = toInt64(m["size"]) / mb
				result.Count      = toInt64(m["count"])
				result.AvgObjSize = toFloat64(m["avgObjSize"])
				if result.SizeMB > 0 {
					result.UsedPct = float64(result.UsedMB) / float64(result.SizeMB) * 100
				}
			}

			// Oplog window: time diff between first and last entry.
			firstCur, err2 := c.Database("local").Collection("oplog.rs").Find(ctx,
				bson.D{}, &options.FindOptions{
					Limit: func() *int64 { v := int64(1); return &v }(),
					Sort:  bson.D{{Key: "$natural", Value: 1}},
					Projection: bson.D{{Key: "ts", Value: 1}},
				},
			)
			lastCur, err3 := c.Database("local").Collection("oplog.rs").Find(ctx,
				bson.D{}, &options.FindOptions{
					Limit: func() *int64 { v := int64(1); return &v }(),
					Sort:  bson.D{{Key: "$natural", Value: -1}},
					Projection: bson.D{{Key: "ts", Value: 1}},
				},
			)
			if err2 == nil && err3 == nil {
				// Decode the BSON Timestamp directly into a typed struct. Routing
				// it through bsonToAny() (which boxes the timestamp into a
				// map[string]uint32) was the reason the window always read 0.
				type oplogTS struct {
					Ts primitive.Timestamp `bson:"ts"`
				}
				var firstDoc, lastDoc oplogTS
				if firstCur.Next(ctx) { firstCur.Decode(&firstDoc) }
				if lastCur.Next(ctx)  { lastCur.Decode(&lastDoc) }
				firstCur.Close(ctx); lastCur.Close(ctx)
				ft, lt := int64(firstDoc.Ts.T), int64(lastDoc.Ts.T)
				result.FirstTS = ft
				result.LastTS  = lt
				if lt > ft {
					result.WindowHours = float64(lt-ft) / 3600
					// Average write rate (MB/h) over the window and the projected
					// retention once the oplog reaches its configured max size.
					if result.WindowHours > 0 && result.UsedMB > 0 {
						result.WriteRateMB = float64(result.UsedMB) / result.WindowHours
						if result.WriteRateMB > 0 && result.SizeMB > 0 {
							result.EstRetHours = float64(result.SizeMB) / result.WriteRateMB
						}
					}
				}
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(rsURI)
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].SetName < results[j].SetName })
	jsonOK(w, results)
}

// ── Oplog Write Analysis ───────────────────────────────────────────────────────
// Per-collection breakdown of recent oplog write volume on a single member.
//
// NOTE ON THE TIME FILTER: the "obvious" approach of comparing $ts (a BSON
// Timestamp) against a Date built from $$NOW does NOT work — in MongoDB's BSON
// type ordering a Timestamp always sorts AFTER a Date, so $gte:["$ts", <Date>]
// is unconditionally true and the pipeline would silently scan the entire
// oplog instead of the last N minutes. Instead we anchor to the oplog's own
// clock: take the newest entry's timestamp seconds and build a Timestamp lower
// bound at (newest - minutes*60). That filter is both correct and efficient,
// because oplog.rs is naturally ordered by ts.
//
// $bsonSize requires MongoDB 4.4+. On older servers the aggregation will return
// an error, which is surfaced verbatim to the UI.
func handleOplogAnalyze(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	if uri == "" { jsonErr(w, "uri required", 400); return }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	minutes := 10
	if v := strings.TrimSpace(r.FormValue("minutes")); v != "" {
		var n int
		if c, err := fmt.Sscanf(v, "%d", &n); c == 1 && err == nil { minutes = n }
	}
	if minutes < 1 { minutes = 1 }
	if minutes > 1440 { minutes = 1440 }

	limit := 15
	if v := strings.TrimSpace(r.FormValue("limit")); v != "" {
		var n int
		if c, err := fmt.Sscanf(v, "%d", &n); c == 1 && err == nil { limit = n }
	}
	if limit < 1 { limit = 1 }
	if limit > 50 { limit = 50 }

	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	// This query can be heavy on a busy primary, so give it room but keep a cap.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	coll := c.Database("local").Collection("oplog.rs")

	// Newest entry → anchor for the time window (oplog clock, not wall clock).
	type oplogTS struct {
		Ts primitive.Timestamp `bson:"ts"`
	}
	var newest oplogTS
	{
		cur, e := coll.Find(ctx, bson.D{}, &options.FindOptions{
			Limit:      func() *int64 { v := int64(1); return &v }(),
			Sort:       bson.D{{Key: "$natural", Value: -1}},
			Projection: bson.D{{Key: "ts", Value: 1}},
		})
		if e != nil { jsonErr(w, e.Error(), 500); return }
		if cur.Next(ctx) { cur.Decode(&newest) }
		cur.Close(ctx)
	}
	if newest.Ts.T == 0 {
		jsonOK(w, map[string]interface{}{
			"ok": "1", "minutes": minutes, "from": 0, "to": 0,
			"collections": []interface{}{},
		})
		return
	}

	lowerSecs := int64(newest.Ts.T) - int64(minutes)*60
	if lowerSecs < 0 { lowerSecs = 0 }
	lower := primitive.Timestamp{T: uint32(lowerSecs), I: 0}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "ts", Value: bson.D{{Key: "$gte", Value: lower}}},
			{Key: "op", Value: bson.D{{Key: "$in", Value: bson.A{"i", "u", "d", "c"}}}},
		}}},
		bson.D{{Key: "$addFields", Value: bson.D{
			{Key: "entrySizeBytes", Value: bson.D{{Key: "$bsonSize", Value: "$$ROOT"}}},
		}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$ns"},
			{Key: "totalBytes", Value: bson.D{{Key: "$sum", Value: "$entrySizeBytes"}}},
			{Key: "operationCount", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "collection", Value: "$_id"},
			{Key: "totalMB", Value: bson.D{{Key: "$round", Value: bson.A{
				bson.D{{Key: "$divide", Value: bson.A{"$totalBytes", 1048576}}}, 2,
			}}}},
			{Key: "operationCount", Value: 1},
			{Key: "avgSizeKB", Value: bson.D{{Key: "$round", Value: bson.A{
				bson.D{{Key: "$divide", Value: bson.A{
					bson.D{{Key: "$divide", Value: bson.A{"$totalBytes", "$operationCount"}}}, 1024,
				}}}, 2,
			}}}},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "totalMB", Value: -1}}}},
		bson.D{{Key: "$limit", Value: limit}},
	}

	cur, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil { jsonErr(w, err.Error(), 500); return }
	defer cur.Close(ctx)

	type CollOplog struct {
		Collection     string  `bson:"collection" json:"collection"`
		TotalMB        float64 `bson:"totalMB"    json:"totalMB"`
		OperationCount int64   `bson:"operationCount" json:"operationCount"`
		AvgSizeKB      float64 `bson:"avgSizeKB"  json:"avgSizeKB"`
	}
	rows := []CollOplog{}
	if err := cur.All(ctx, &rows); err != nil { jsonErr(w, err.Error(), 500); return }

	jsonOK(w, map[string]interface{}{
		"ok":          "1",
		"minutes":     minutes,
		"from":        lowerSecs,
		"to":          int64(newest.Ts.T),
		"collections": rows,
	})
}

// ── Slow Query Profiler ───────────────────────────────────────────────────────

func handleGetProfilingLevel(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri    := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	if dbName == "" { jsonErr(w, "db required", 400); return }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res bson.D
	err = c.Database(dbName).RunCommand(ctx, bson.D{{Key: "profile", Value: -1}}).Decode(&res)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	jsonOK(w, bsonToAny(res))
}

func handleSetProfilingLevel(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri    := formURI(r)
	dbName := strings.TrimSpace(r.FormValue("db"))
	level  := 0
	fmt.Sscanf(r.FormValue("level"), "%d", &level)
	slowMs := int64(100)
	fmt.Sscanf(r.FormValue("slowMs"), "%d", &slowMs)
	sampleRateStr := strings.TrimSpace(r.FormValue("sampleRate"))
	filterJSON    := strings.TrimSpace(r.FormValue("filter"))
	if dbName == "" { jsonErr(w, "db required", 400); return }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// db.runCommand({profile: level, slowms: …, sampleRate: …, filter: …})
	// slowms is always sent so its current UI value sticks. sampleRate and
	// filter are sent only when the client opts in — that keeps existing
	// server-side values intact when the user is only tweaking level/slowms.
	cmd := bson.D{
		{Key: "profile", Value: level},
		{Key: "slowms",  Value: slowMs},
	}

	if sampleRateStr != "" {
		var sr float64
		if _, perr := fmt.Sscanf(sampleRateStr, "%f", &sr); perr == nil {
			if sr < 0 { sr = 0 }
			if sr > 1 { sr = 1 }
			cmd = append(cmd, bson.E{Key: "sampleRate", Value: sr})
		}
	}

	// filter accepts either a query document (JSON object) or the literal
	// string "unset" which removes the stored filter — see
	// https://www.mongodb.com/docs/v7.0/reference/method/db.setProfilingLevel/
	if filterJSON != "" {
		if filterJSON == "unset" || filterJSON == "\"unset\"" {
			cmd = append(cmd, bson.E{Key: "filter", Value: "unset"})
		} else {
			var fmap map[string]interface{}
			if jerr := json.Unmarshal([]byte(filterJSON), &fmap); jerr != nil {
				jsonErr(w, "invalid filter JSON: "+jerr.Error(), 400)
				return
			}
			cmd = append(cmd, bson.E{Key: "filter", Value: fmap})
		}
	}

	var res bson.D
	err = c.Database(dbName).RunCommand(ctx, cmd).Decode(&res)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	jsonOK(w, bsonToAny(res))
}

func handleProfilerEntries(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri              := formURI(r)
	dbName           := strings.TrimSpace(r.FormValue("db"))
	ns               := strings.TrimSpace(r.FormValue("ns"))
	op               := strings.TrimSpace(r.FormValue("op"))
	cmdType          := strings.TrimSpace(r.FormValue("cmdType"))
	planSummary      := strings.TrimSpace(r.FormValue("planSummary"))
	impUser          := strings.TrimSpace(r.FormValue("user"))
	minMs            := int64(0)
	fmt.Sscanf(r.FormValue("minMs"), "%d", &minMs)
	minDocsExamined  := int64(0)
	fmt.Sscanf(r.FormValue("minDocsExamined"), "%d", &minDocsExamined)
	minCpuMs         := int64(0)
	fmt.Sscanf(r.FormValue("minCpuMs"), "%d", &minCpuMs)
	limit            := int64(100)
	fmt.Sscanf(r.FormValue("limit"), "%d", &limit)
	if limit > 500 { limit = 500 }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)
	c, err := getClient(uri)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	filter := bson.D{}
	if minMs > 0 {
		filter = append(filter, bson.E{Key: "millis", Value: bson.D{{Key: "$gte", Value: minMs}}})
	}
	if minDocsExamined > 0 {
		filter = append(filter, bson.E{Key: "docsExamined", Value: bson.D{{Key: "$gte", Value: minDocsExamined}}})
	}
	if minCpuMs > 0 {
		// cpuNanos is stored in nanoseconds; convert the UI's ms input.
		filter = append(filter, bson.E{Key: "cpuNanos", Value: bson.D{{Key: "$gte", Value: minCpuMs * 1_000_000}}})
	}
	if ns != "" {
		filter = append(filter, bson.E{Key: "ns", Value: bson.D{{Key: "$regex", Value: regexp.QuoteMeta(ns)}}})
	}
	if op != "" {
		filter = append(filter, bson.E{Key: "op", Value: op})
	}
	if planSummary != "" {
		// Anchor at start so "IXSCAN" matches `IXSCAN { age: 1 }` etc.
		filter = append(filter, bson.E{Key: "planSummary", Value: bson.D{
			{Key: "$regex", Value: "^" + regexp.QuoteMeta(planSummary)},
			{Key: "$options", Value: "i"},
		}})
	}
	if impUser != "" {
		// The user behind a profiled op may live in several places depending
		// on MongoDB version and sharded vs. plain RS:
		//   - command.$audit.$impersonatedUser.user — mongos forwarded with impersonation
		//   - effectiveUsers[].user — modern (5.0+) auth representation on the entry itself
		//   - user — older single-string form
		// Use a regex-contains match so the input is forgiving (case-insensitive).
		userRx := bson.D{
			{Key: "$regex", Value: regexp.QuoteMeta(impUser)},
			{Key: "$options", Value: "i"},
		}
		filter = append(filter, bson.E{Key: "$or", Value: bson.A{
			bson.D{{Key: "command.$audit.$impersonatedUser.user", Value: userRx}},
			bson.D{{Key: "effectiveUsers.user", Value: userRx}},
			bson.D{{Key: "user", Value: userRx}},
		}})
	}
	if cmdType != "" {
		// Map a user-friendly command name to legacy `op` values plus the
		// modern `command.<name>` first-key form, OR'd together. Profile
		// entries for sharded clusters almost always use op="command" with
		// the actual command name as the first key of `command`.
		ct := strings.ToLower(cmdType)
		var clauses bson.A
		add := func(d bson.D) { clauses = append(clauses, d) }
		exists := func(key string) bson.D {
			return bson.D{{Key: key, Value: bson.D{{Key: "$exists", Value: true}}}}
		}
		switch ct {
		case "find":
			add(bson.D{{Key: "op", Value: "query"}})
			add(exists("command.find"))
		case "aggregate":
			add(exists("command.aggregate"))
		case "count":
			add(exists("command.count"))
		case "distinct":
			add(exists("command.distinct"))
		case "findandmodify":
			add(exists("command.findAndModify"))
			add(exists("command.findandmodify"))
		case "update":
			add(bson.D{{Key: "op", Value: "update"}})
			add(exists("command.update"))
		case "delete", "remove":
			add(bson.D{{Key: "op", Value: "remove"}})
			add(exists("command.delete"))
		case "insert":
			add(bson.D{{Key: "op", Value: "insert"}})
			add(exists("command.insert"))
		case "getmore":
			add(bson.D{{Key: "op", Value: "getmore"}})
			add(exists("command.getMore"))
		}
		if len(clauses) > 0 {
			filter = append(filter, bson.E{Key: "$or", Value: clauses})
		}
	}

	cur, err := c.Database(dbName).Collection("system.profile").Find(ctx, filter,
		&options.FindOptions{
			Sort:  bson.D{{Key: "millis", Value: -1}},
			Limit: &limit,
		},
	)
	if err != nil { jsonErr(w, err.Error(), 500); return }
	defer cur.Close(ctx)

	var rawDocs []bson.D
	cur.All(ctx, &rawDocs)
	entries := make([]interface{}, len(rawDocs))
	for i, d := range rawDocs { entries[i] = bsonToAny(d) }
	jsonOK(w, entries)
}

// handleExplain runs db.runCommand({explain: <cmd>, verbosity: ...}) for a
// command captured by the profiler. Profile entries contain session and
// routing metadata (lsid, $clusterTime, $db, …) that MongoDB rejects when
// nested under explain, so those fields are stripped first. The explainable
// operation (find / aggregate / count / distinct / findAndModify / update /
// delete / mapReduce) must be the first key of the inner command — we
// reorder accordingly. Legacy op="query" profile entries with only a raw
// filter document are reconstructed into a find command from the namespace.
func handleExplain(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri       := formURI(r)
	dbName    := strings.TrimSpace(r.FormValue("db"))
	cmdJSON   := r.FormValue("command")
	opType    := strings.ToLower(strings.TrimSpace(r.FormValue("op")))
	ns        := strings.TrimSpace(r.FormValue("ns"))
	verbosity := strings.TrimSpace(r.FormValue("verbosity"))

	if dbName == "" {
		jsonErr(w, "db required", 400)
		return
	}
	if strings.TrimSpace(cmdJSON) == "" || cmdJSON == "{}" {
		jsonErr(w, "command is empty — nothing to explain", 400)
		return
	}
	if verbosity == "" {
		verbosity = "executionStats"
	}
	switch verbosity {
	case "queryPlanner", "executionStats", "allPlansExecution":
	default:
		jsonErr(w, "verbosity must be queryPlanner, executionStats, or allPlansExecution", 400)
		return
	}

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	// Parse the original command as a map.
	var cmdMap map[string]interface{}
	if err := json.Unmarshal([]byte(cmdJSON), &cmdMap); err != nil {
		jsonErr(w, "invalid command JSON: "+err.Error(), 400)
		return
	}

	// Drop session / cluster / routing / API metadata. These are added by the
	// driver or by mongos when forwarding the request to a shard, and the
	// server rejects (or mis-routes) them when wrapped in explain.
	// writeConcern is specifically rejected by aggregate-explain ("Aggregation
	// explain does not support the 'writeConcern' option") and is meaningless
	// for any explain (explain never writes), so it's always stripped.
	for _, k := range []string{
		"lsid", "$clusterTime", "$db", "$audit", "$readPreference", "$client",
		"$configServerState", "$queryOptions",
		"$configTime", "$topologyTime",
		"txnNumber", "autocommit", "startTransaction", "signature",
		"apiVersion", "apiStrict", "apiDeprecationErrors",
		"needsMerge", "fromMongos", "mayBypassWriteBlocking", "useBypassDocumentValidation",
		"shardVersion", "databaseVersion", "requestGossipRoutingCache",
		"maxTimeMSOpOnly",
		"writeConcern",
		"clientOperationKey",
		"$readOnce", "readOnce",
	} {
		delete(cmdMap, k)
	}

	// The `let` block is legitimate user-facing functionality (lets you bind
	// variables visible to aggregation/find expressions as `$$name`), but
	// mongos also uses it internally to ferry reserved system variables —
	// CLUSTER_TIME, NOW, etc. — to shards. Those names are reserved at the
	// server; any attempt to set them via let yields:
	//   (Location4738901) Attempt to set internal constant: CLUSTER_TIME
	// Strip the reserved keys while keeping any user-supplied variables. If
	// nothing user-supplied remains, drop the let block entirely.
	if letField, ok := cmdMap["let"]; ok {
		if letMap, ok := letField.(map[string]interface{}); ok {
			reserved := []string{
				"NOW", "CLUSTER_TIME", "JS_SCOPE", "IS_MR", "USER_ROLES",
				"ROOT", "CURRENT", "REMOVE", "DESCEND", "PRUNE", "KEEP",
				"SEARCH_META",
			}
			for _, k := range reserved {
				delete(letMap, k)
			}
			if len(letMap) == 0 {
				delete(cmdMap, "let")
			} else {
				cmdMap["let"] = letMap
			}
		}
	}

	// Locate the explainable op (case-insensitive). Order of preference is
	// fixed so a profile entry containing both "find" and (say) "count" still
	// resolves deterministically.
	explainableOps := []string{
		"find", "aggregate", "count", "distinct",
		"findAndModify", "delete", "update", "mapReduce",
	}
	var primaryKey string
	var primaryVal interface{}
	for _, op := range explainableOps {
		for k, v := range cmdMap {
			if strings.EqualFold(k, op) {
				primaryKey = k
				primaryVal = v
				break
			}
		}
		if primaryKey != "" {
			break
		}
	}

	// Fallback: legacy op="query" entries have just a filter document.
	// Reconstruct a find command from the namespace.
	if primaryKey == "" && opType == "query" {
		collName := ns
		if dotIdx := strings.Index(ns, "."); dotIdx >= 0 {
			collName = ns[dotIdx+1:]
		}
		if collName == "" {
			jsonErr(w, "cannot reconstruct find command: missing collection name in ns", 400)
			return
		}
		// In the legacy profile shape the entry's stored object may be the
		// filter directly, or may be wrapped under $query / filter / $orderby.
		filter := map[string]interface{}{}
		sort   := map[string]interface{}{}
		for k, v := range cmdMap {
			switch {
			case strings.EqualFold(k, "$query"), strings.EqualFold(k, "filter"):
				if m, ok := v.(map[string]interface{}); ok {
					for fk, fv := range m { filter[fk] = fv }
				}
			case strings.EqualFold(k, "$orderby"), strings.EqualFold(k, "sort"):
				if m, ok := v.(map[string]interface{}); ok {
					for sk, sv := range m { sort[sk] = sv }
				}
			default:
				filter[k] = v
			}
		}
		cmdMap = map[string]interface{}{"find": collName}
		if len(filter) > 0 { cmdMap["filter"] = filter }
		if len(sort) > 0   { cmdMap["sort"]   = sort   }
		primaryKey = "find"
		primaryVal = collName
	}

	if primaryKey == "" {
		jsonErr(w, "command is not directly explainable (no find / aggregate / count / distinct / findAndModify / update / delete / mapReduce key found after stripping metadata)", 400)
		return
	}

	// Aggregate-specific guard: a profile entry whose pipeline starts with
	// $mergeCursors is the post-merge fragment recorded on the merging shard
	// of a sharded aggregate. The $mergeCursors stage holds cursor IDs from
	// the original cross-shard execution — those cursors no longer exist, so
	// the pipeline cannot be re-executed or re-explained. Refuse early with a
	// clear message instead of letting the server return a cryptic parse
	// error. Note: cursor field is intentionally NOT stripped — aggregate
	// accepts cursor under the explain wrapper, and keeping it protects
	// against the case where mongos unfolds explain into the embedded form
	// and the inner command is then parsed as a plain aggregate.
	if strings.EqualFold(primaryKey, "aggregate") {
		if pipeline, ok := cmdMap["pipeline"].([]interface{}); ok && len(pipeline) > 0 {
			if firstStage, ok := pipeline[0].(map[string]interface{}); ok {
				if _, hasMC := firstStage["$mergeCursors"]; hasMC {
					jsonErr(w,
						"this profile entry is the post-merge fragment of a sharded aggregate (recorded on the merging shard); its pipeline starts with $mergeCursors which references cursor IDs from past execution on other shards. Those cursors are gone, so the pipeline cannot be re-explained. To explain the original query, run it against your mongos directly (e.g. db.collection.explain('executionStats').aggregate([...])).",
						400)
					return
				}
			}
		}
	}

	// Build the inner command with the primary op first (required), then the
	// rest of the keys.
	innerCmd := bson.D{{Key: primaryKey, Value: primaryVal}}
	for k, v := range cmdMap {
		if k != primaryKey {
			innerCmd = append(innerCmd, bson.E{Key: k, Value: v})
		}
	}

	explainCmd := bson.D{
		{Key: "explain",   Value: innerCmd},
		{Key: "verbosity", Value: verbosity},
	}

	res, err := runCmd(uri, dbName, explainCmd)
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, res)
}

// ── Log Viewer ────────────────────────────────────────────────────────────────

func handleGetLog(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri  := formURI(r)
	kind := r.FormValue("kind")
	if kind == "" { kind = "global" }

	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	res, err := runCmd(uri, "admin", bson.D{{Key: "getLog", Value: kind}})
	if err != nil { jsonErr(w, err.Error(), 500); return }
	jsonOK(w, res)
}

// ── Security Status ───────────────────────────────────────────────────────────
// Per-node authentication posture (is access control enabled?) plus
// authentication-mechanism usage counters from serverStatus. Works for
// standalone, replica set, and sharded topologies: the frontend passes the set
// of nodes to inspect via node_uri[]/node_id[]/node_role[]; if none are given
// we fall back to the single connection URI (standalone).

type SecMechStat struct {
	Mechanism         string `json:"mechanism"`
	AuthReceived      int64  `json:"authReceived"`
	AuthSuccessful    int64  `json:"authSuccessful"`
	SpecReceived      int64  `json:"specReceived"`
	SpecSuccessful    int64  `json:"specSuccessful"`
	ClusterReceived   int64  `json:"clusterReceived"`
	ClusterSuccessful int64  `json:"clusterSuccessful"`
}

type SecNode struct {
	ID              string        `json:"id"`
	Role            string        `json:"role"`
	Host            string        `json:"host"`
	AuthEnabled     bool          `json:"authEnabled"`
	Authorization   string        `json:"authorization"`
	KeyFile         bool          `json:"keyFile"`
	ClusterAuthMode string        `json:"clusterAuthMode"`
	TLSMode         string        `json:"tlsMode"`
	Detected        string        `json:"detected"`
	Version         string        `json:"version"`
	Mechanisms      []SecMechStat `json:"mechanisms"`
	Error           string        `json:"error,omitempty"`
}

func handleSecurityStatus(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	shardCreds := parseShardCredentials(r)

	ids := r.Form["node_id"]
	roles := r.Form["node_role"]
	uris := r.Form["node_uri"]

	type target struct{ id, role, uri string }
	var targets []target
	for i, u := range uris {
		if strings.TrimSpace(u) == "" {
			continue
		}
		t := target{uri: u}
		if i < len(ids) {
			t.id = ids[i]
		}
		if i < len(roles) {
			t.role = roles[i]
		}
		targets = append(targets, t)
	}
	// Standalone / fallback: no node list supplied → inspect the connection URI.
	if len(targets) == 0 {
		if u := formURI(r); u != "" {
			targets = append(targets, target{id: "standalone", role: "standalone", uri: u})
		}
	}
	if len(targets) == 0 {
		jsonErr(w, "uri required", 400)
		return
	}

	var mu sync.Mutex
	var nodes []SecNode
	var wg sync.WaitGroup

	for _, t := range targets {
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			finalURI := applyShardCredentials(t.uri, shardCreds)
			node := SecNode{
				ID: t.id, Role: t.role, Host: hostFromURI(t.uri),
				Authorization: "<not set>", Mechanisms: []SecMechStat{},
			}
			if node.ID == "" {
				node.ID = node.Host
			}

			// ── Authentication posture via getCmdLineOpts ──
			opts, err := runCmd(finalURI, "admin", bson.D{{Key: "getCmdLineOpts", Value: 1}})
			if err != nil {
				le := strings.ToLower(err.Error())
				if strings.Contains(le, "not authorized") || strings.Contains(le, "unauthorized") ||
					strings.Contains(le, "requires authentication") || strings.Contains(le, "authentication failed") {
					// The command itself is gated → access control IS enabled.
					node.AuthEnabled = true
					node.Authorization = "enabled (inferred)"
					node.Detected = "command blocked — authentication required"
				} else {
					node.Error = err.Error()
					mu.Lock()
					nodes = append(nodes, node)
					mu.Unlock()
					return
				}
			} else {
				hasAuthFlag := false
				for _, a := range toSlice(opts["argv"]) {
					if s, _ := a.(string); s == "--auth" {
						hasAuthFlag = true
					}
				}
				if parsed := toMap(opts["parsed"]); parsed != nil {
					if sec := toMap(parsed["security"]); sec != nil {
						if az, ok := sec["authorization"].(string); ok && az != "" {
							node.Authorization = az
							if strings.EqualFold(az, "enabled") {
								node.AuthEnabled = true
								node.Detected = "security.authorization = enabled"
							}
						}
						if kf, ok := sec["keyFile"].(string); ok && kf != "" {
							node.KeyFile = true
							if !node.AuthEnabled {
								node.AuthEnabled = true
								node.Detected = "security.keyFile set (internal auth)"
							}
						}
						if cam, ok := sec["clusterAuthMode"].(string); ok {
							node.ClusterAuthMode = cam
						}
					}
					// TLS / SSL mode (bonus security context)
					if net := toMap(parsed["net"]); net != nil {
						if tls := toMap(net["tls"]); tls != nil {
							if m, ok := tls["mode"].(string); ok {
								node.TLSMode = m
							}
						}
						if node.TLSMode == "" {
							if ssl := toMap(net["ssl"]); ssl != nil {
								if m, ok := ssl["mode"].(string); ok {
									node.TLSMode = m
								}
							}
						}
					}
				}
				if !node.AuthEnabled && hasAuthFlag {
					node.AuthEnabled = true
					node.Detected = "--auth command-line flag"
				}
				if !node.AuthEnabled && node.Detected == "" {
					node.Detected = "no access control configured"
				}
			}

			// ── Mechanism usage + version via serverStatus ──
			if ss, err2 := runCmd(finalURI, "admin", bson.D{{Key: "serverStatus", Value: 1}}); err2 == nil {
				if v, ok := ss["version"].(string); ok {
					node.Version = v
				}
				if sec := toMap(ss["security"]); sec != nil {
					if auth := toMap(sec["authentication"]); auth != nil {
						if mechs := toMap(auth["mechanisms"]); mechs != nil {
							names := make([]string, 0, len(mechs))
							for name := range mechs {
								names = append(names, name)
							}
							sort.Strings(names)
							for _, name := range names {
								md := toMap(mechs[name])
								if md == nil {
									continue
								}
								ms := SecMechStat{Mechanism: name}
								if a := toMap(md["authenticate"]); a != nil {
									ms.AuthReceived = toInt64(a["received"])
									ms.AuthSuccessful = toInt64(a["successful"])
								}
								if sp := toMap(md["speculativeAuthenticate"]); sp != nil {
									ms.SpecReceived = toInt64(sp["received"])
									ms.SpecSuccessful = toInt64(sp["successful"])
								}
								if cl := toMap(md["clusterAuthenticate"]); cl != nil {
									ms.ClusterReceived = toInt64(cl["received"])
									ms.ClusterSuccessful = toInt64(cl["successful"])
								}
								node.Mechanisms = append(node.Mechanisms, ms)
							}
						}
					}
				}
			}

			mu.Lock()
			nodes = append(nodes, node)
			mu.Unlock()
		}(t)
	}
	wg.Wait()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	jsonOK(w, map[string]interface{}{"nodes": nodes})
}

// ── WiredTiger Stats ──────────────────────────────────────────────────────────

func handleWiredTiger(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	shardCreds := parseShardCredentials(r)
	uri = applyShardCredentials(uri, shardCreds)

	res, err := runCmd(uri, "admin", bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil { jsonErr(w, err.Error(), 500); return }

	out := map[string]interface{}{}
	if wt := toMap(res["wiredTiger"]); wt != nil { out["wiredTiger"] = wt }
	if tc := toMap(res["tcmalloc"]);  tc != nil  { out["tcmalloc"]  = tc  }
	if mem := toMap(res["mem"]);      mem != nil  { out["mem"]       = mem }
	if extra := toMap(res["extra_info"]); extra != nil { out["extra_info"] = extra }
	if ver, ok := res["version"]; ok  { out["version"] = ver }
	if host, ok := res["host"]; ok    { out["host"]    = host }
	if proc, ok := res["process"]; ok { out["process"] = proc }
	jsonOK(w, out)
}

// ── Ping ──────────────────────────────────────────────────────────────────────

func handlePing(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	uri := formURI(r)
	if uri == "" {
		jsonErr(w, "uri required", 400)
		return
	}
	start := time.Now()
	_, err := getClient(uri)
	ms := time.Since(start).Milliseconds()
	if err != nil {
		jsonErr(w, err.Error(), 500)
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "ms": ms})
}

// ── helpers ───────────────────────────────────────────────────────────────────

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

// tsSecondsFromAny extracts the seconds component (T) from the value that
// bsonToAny() produces for a BSON Timestamp, i.e.
//   {"$timestamp": {"t": <uint32>, "i": <uint32>}}
// The inner map is created as map[string]uint32 by bsonToAny, so a plain
// toMap() (which only matches map[string]interface{}) would miss it — which
// is exactly why the oplog "Window" used to always read 0. This helper copes
// with both shapes.
func tsSecondsFromAny(v interface{}) int64 {
	m, ok := v.(map[string]interface{})
	if !ok {
		return 0
	}
	inner, ok := m["$timestamp"]
	if !ok {
		return 0
	}
	switch t := inner.(type) {
	case map[string]uint32:
		return int64(t["t"])
	case map[string]interface{}:
		return toInt64(t["t"])
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	}
	return 0
}

func toMap(v interface{}) map[string]interface{} {
	m, _ := v.(map[string]interface{})
	return m
}

func toSlice(v interface{}) []interface{} {
	s, _ := v.([]interface{})
	return s
}

// ── Individual RS member enumeration ─────────────────────────────────────────
// Returns a flat list of {id, role, uri} for every individual mongod host
// reachable from the known RS URIs — so the frontend can build a per-host selector.

type MemberNode struct {
	ID   string `json:"id"`
	Role string `json:"role"` // "shard-primary", "shard-secondary", "configsvr-primary", etc.
	URI  string `json:"uri"`  // direct single-host URI: mongodb://host:port
	RS   string `json:"rs"`   // replica set name
}

func handleRsMembers(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	rsURIs := r.Form["rs_uri"]
	if len(rsURIs) == 0 {
		rsURIs = []string{formURI(r)}
	}

	// Parse per-shard credentials from form
	shardCreds := parseShardCredentials(r)

	var mu sync.Mutex
	var members []MemberNode
	var wg sync.WaitGroup

	for _, rsURI := range rsURIs {
		wg.Add(1)
		go func(rsURI string) {
			defer wg.Done()
			// Apply shard-specific credentials if available
			finalURI := applyShardCredentials(rsURI, shardCreds)
			
			// Extract credentials from the final URI to inject into direct connections
			username, password := parseCredentials(finalURI)
			
			// Get role from topology nodes if possible
			res, err := runCmd(finalURI, "admin", bson.D{{Key: "replSetGetStatus", Value: 1}})
			if err != nil {
				return
			}
			rsName, _ := res["set"].(string)
			for _, m := range toSlice(res["members"]) {
				mm := toMap(m)
				if mm == nil {
					continue
				}
				name, _ := mm["name"].(string)
				stateStr, _ := mm["stateStr"].(string)
				if name == "" {
					continue
				}
				role := strings.ToLower(stateStr) // "primary", "secondary", etc.
				// directConnection=true forces the driver to talk to THIS specific
				// host and not re-route to the primary. Essential for per-member
				// serverStatus (e.g. flow control enabled differs per member).
				baseURI := "mongodb://" + name + "/?directConnection=true"
				directURI := injectCredentials(baseURI, username, password)
				mu.Lock()
				members = append(members, MemberNode{
					ID:   rsName + "/" + name,
					Role: role,
					URI:  directURI,
					RS:   rsName,
				})
				mu.Unlock()
			}
		}(rsURI)
	}
	wg.Wait()
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	jsonOK(w, members)
}

var tmpl *template.Template

// generateSelfSignedCert creates a self-signed certificate and key if they don't exist
func generateSelfSignedCert(certFile, keyFile string) error {
	// Check if cert and key already exist
	if _, err := os.Stat(certFile); err == nil {
		if _, err := os.Stat(keyFile); err == nil {
			debugLog("Using existing certificates: %s, %s", certFile, keyFile)
			return nil
		}
	}

	debugLog("Generating self-signed certificate: %s, %s", certFile, keyFile)

	// Generate RSA key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %w", err)
	}

	// Create certificate template
	cert := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"MongoAdmin"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0), // Valid for 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "127.0.0.1"},
	}

	// Create certificate
	certBytes, err := x509.CreateCertificate(rand.Reader, &cert, &cert, &privateKey.PublicKey, privateKey)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}

	// Write certificate to file
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	if err := os.WriteFile(certFile, certPEM, 0644); err != nil {
		return fmt.Errorf("failed to write certificate file: %w", err)
	}

	// Write private key to file
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyBytes,
	})
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}

	log.Printf("✅ Self-signed certificate generated: %s, %s", certFile, keyFile)
	return nil
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, map[string]interface{}{
		"ViewOnly": viewOnly,
		"Version":  version,
	})
}

// readOnlyGuard wraps an http.HandlerFunc and returns 403 with a JSON error
// when the server is running in --view-only mode.
func readOnlyGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if viewOnly {
			jsonErr(w, "Server is running in view-only mode — write operations are disabled", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func main() {
	// Parse command-line flags
	debug := flag.Bool("debug", false, "Enable debug logging")
	showVersion := flag.Bool("version", false, "Show version")
	tcpPort := flag.String("tcp_port", "8787", "TCP port to listen on (default: 8787)")
	tlsEnabled := flag.Bool("tls", false, "Enable HTTPS with self-signed certificate")
	certFile := flag.String("cert", "mongoadmin.crt", "Path to TLS certificate file")
	keyFile := flag.String("key", "mongoadmin.key", "Path to TLS key file")
	viewOnlyFlag := flag.Bool("view-only", false, "Disable all write/mutating operations (read-only mode)")
	flag.Parse()

	if *showVersion {
		fmt.Printf("MongoAdmin version %s\n", version)
		os.Exit(0)
	}

	debugMode = *debug
	viewOnly  = *viewOnlyFlag

	src, err := os.ReadFile("templates/index.html")
	if err != nil {
		log.Fatal("Cannot read template: ", err)
	}
	tmpl = template.Must(template.New("index").Parse(string(src)))

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	mux.HandleFunc("/api/ping",                 handlePing)
	mux.HandleFunc("/api/topology",             handleTopology)
	mux.HandleFunc("/api/rs/config",            handleRsConfig)
	mux.HandleFunc("/api/rs/status/all",        handleAllRsStatus)
	mux.HandleFunc("/api/rs/members",           handleRsMembers)
	mux.HandleFunc("/api/sh/status",            handleShStatus)
	mux.HandleFunc("/api/sh/balancer",          readOnlyGuard(handleBalancerToggle))
	mux.HandleFunc("/api/sh/write-concern",     readOnlyGuard(handleSetWriteConcern))
	mux.HandleFunc("/api/rs/flow-control",      readOnlyGuard(handleFlowControl))
	mux.HandleFunc("/api/server/status",        handleServerStatus)
	mux.HandleFunc("/api/server/metrics",       handleServerMetrics)
	mux.HandleFunc("/api/server/hostinfo",       handleHostInfo)
	mux.HandleFunc("/api/server/top-commands",  handleTopCommands)
	mux.HandleFunc("/api/current-op",           handleCurrentOp)
	mux.HandleFunc("/api/kill-op",              readOnlyGuard(handleKillOp))
	mux.HandleFunc("/api/rs/member/patch",      readOnlyGuard(handlePatchMember))
	mux.HandleFunc("/api/server/getparam",         handleGetParameter)
	mux.HandleFunc("/api/db/stats",                handleDbStats)
	mux.HandleFunc("/api/db/sharded-collections",  handleShardedCollections)
	mux.HandleFunc("/api/db/chunk-distribution",   handleChunkDistribution)
	mux.HandleFunc("/api/db/coll-stats",           handleCollStats)
	mux.HandleFunc("/api/db/indexes",              handleListIndexes)
	mux.HandleFunc("/api/db/drop-index",           readOnlyGuard(handleDropIndex))
	mux.HandleFunc("/api/db/oplog-stats",          handleOplogStats)
	mux.HandleFunc("/api/db/oplog-analyze",        handleOplogAnalyze)
	mux.HandleFunc("/api/db/profiler-level",       handleGetProfilingLevel)
	mux.HandleFunc("/api/db/profiler-level-set",   readOnlyGuard(handleSetProfilingLevel))
	mux.HandleFunc("/api/db/profiler-entries",     handleProfilerEntries)
	mux.HandleFunc("/api/db/explain",              handleExplain)
	mux.HandleFunc("/api/db/log",                  handleGetLog)
	mux.HandleFunc("/api/db/security-status",      handleSecurityStatus)
	mux.HandleFunc("/api/db/wiredtiger",           handleWiredTiger)
	mux.HandleFunc("/api/user/roles",              handleGetRoles)
	mux.HandleFunc("/api/user/role-detail",        handleGetRoleDetail)
	mux.HandleFunc("/api/user/update-role",        readOnlyGuard(handleUpdateRole))
	mux.HandleFunc("/api/user/delete-role",        readOnlyGuard(handleDeleteRole))
	mux.HandleFunc("/api/user/create-role",        readOnlyGuard(handleCreateRole))
	mux.HandleFunc("/api/user/users",              handleGetUsers)
	mux.HandleFunc("/api/user/connection-status",  handleConnectionStatus)
	mux.HandleFunc("/api/user/create-user",        readOnlyGuard(handleCreateUser))
	mux.HandleFunc("/api/user/delete-user",        readOnlyGuard(handleDeleteUser))
	mux.HandleFunc("/api/user/set-user-roles",     readOnlyGuard(handleSetUserRoles))
	mux.HandleFunc("/api/user/update-user-roles",  readOnlyGuard(handleUpdateUserRoles))
	mux.HandleFunc("/api/user/check-presence",     handleCheckPresence)
	mux.HandleFunc("/api/user/sync-user",          readOnlyGuard(handleSyncUser))
	mux.HandleFunc("/api/user/sync-role",          readOnlyGuard(handleSyncRole))

	// Validate port
	addr := ":" + *tcpPort
	if !strings.HasPrefix(*tcpPort, ":") && *tcpPort != "" {
		addr = ":" + *tcpPort
	}

	// Handle TLS
	if *tlsEnabled {
		if err := generateSelfSignedCert(*certFile, *keyFile); err != nil {
			log.Fatalf("Failed to generate/load certificates: %v", err)
		}
		fmt.Printf("🍃 MongoAdmin listening on https://localhost%s\n", addr)
		fmt.Printf("⚠️  Using self-signed certificate (ignore browser warnings)\n")
		log.Fatal(http.ListenAndServeTLS(addr, *certFile, *keyFile, mux))
	} else {
		fmt.Printf("🍃 MongoAdmin listening on http://localhost%s\n", addr)
		log.Fatal(http.ListenAndServe(addr, mux))
	}
}
