package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aethiopicuschan/flashkv/version"
)

var casCounter int64

// var verbosityLevel int

// Item represents a cached item.
type Item struct {
	value     []byte
	flags     int
	exptime   int64 // 0 means no expiration.
	casUnique int64 // Unique CAS identifier.
}

func isExpired(item *Item) bool {
	return item.exptime > 0 && time.Now().Unix() > item.exptime
}

// Shard holds part of the data with its own lock.
type Shard struct {
	mu    sync.RWMutex
	items map[string]*Item
}

// Store contains multiple shards.
type Store struct {
	shards []*Shard
	count  int
}

// NewStore returns a new Store with the given shard count.
func New(shardCount int) *Store {
	shards := make([]*Shard, shardCount)
	for i := range shardCount {
		shards[i] = &Shard{
			items: make(map[string]*Item),
		}
	}
	return &Store{
		shards: shards,
		count:  shardCount,
	}
}

// hashKey calculates a simple FNV-1a hash of the key.
func hashKey(key string) uint32 {
	var hash uint32 = 2166136261
	for i := range key {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return hash
}

// getShard returns the shard corresponding to a given key.
func (s *Store) getShard(key string) *Shard {
	h := hashKey(key)
	return s.shards[int(h)%s.count]
}

// Set stores an item regardless of existing value.
func (s *Store) Set(key string, flags int, exptime int64, value []byte) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if exptime > 0 {
		exptime = time.Now().Unix() + exptime
	}
	newCas := atomic.AddInt64(&casCounter, 1)
	shard.items[key] = &Item{value: value, flags: flags, exptime: exptime, casUnique: newCas}
}

// Get retrieves an item by key.
func (s *Store) Get(key string) (*Item, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	item, ok := shard.items[key]
	if ok && isExpired(item) {
		ok = false
	}
	shard.mu.RUnlock()
	return item, ok
}

// Delete removes an item.
func (s *Store) Delete(key string) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	_, ok := shard.items[key]
	if ok {
		delete(shard.items, key)
	}
	return ok
}

// Add stores data only if the key does not exist.
func (s *Store) Add(key string, flags int, exptime int64, value []byte) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if existing, ok := shard.items[key]; ok && !isExpired(existing) {
		return false
	}
	if exptime > 0 {
		exptime = time.Now().Unix() + exptime
	}
	newCas := atomic.AddInt64(&casCounter, 1)
	shard.items[key] = &Item{value: value, flags: flags, exptime: exptime, casUnique: newCas}
	return true
}

// Replace updates an item only if the key exists.
func (s *Store) Replace(key string, flags int, exptime int64, value []byte) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if existing, ok := shard.items[key]; !ok || isExpired(existing) {
		return false
	}
	if exptime > 0 {
		exptime = time.Now().Unix() + exptime
	}
	newCas := atomic.AddInt64(&casCounter, 1)
	shard.items[key] = &Item{value: value, flags: flags, exptime: exptime, casUnique: newCas}
	return true
}

// Append adds data to the end of an existing item.
func (s *Store) Append(key string, data []byte) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	item, ok := shard.items[key]
	if !ok || isExpired(item) {
		return false
	}
	item.value = append(item.value, data...)
	item.casUnique = atomic.AddInt64(&casCounter, 1)
	return true
}

// Prepend adds data to the beginning of an existing item.
func (s *Store) Prepend(key string, data []byte) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	item, ok := shard.items[key]
	if !ok || isExpired(item) {
		return false
	}
	newValue := make([]byte, len(data)+len(item.value))
	copy(newValue, data)
	copy(newValue[len(data):], item.value)
	item.value = newValue
	item.casUnique = atomic.AddInt64(&casCounter, 1)
	return true
}

// Cas (Check-And-Set) updates an item only if the provided CAS value matches.
func (s *Store) Cas(key string, flags int, exptime int64, value []byte, casUnique int64) string {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	item, ok := shard.items[key]
	if !ok || isExpired(item) {
		return "NOT_FOUND\r\n"
	}
	if item.casUnique != casUnique {
		return "EXISTS\r\n"
	}
	if exptime > 0 {
		exptime = time.Now().Unix() + exptime
	}
	newCas := atomic.AddInt64(&casCounter, 1)
	shard.items[key] = &Item{value: value, flags: flags, exptime: exptime, casUnique: newCas}
	return "STORED\r\n"
}

// Touch updates the expiration time of an item.
func (s *Store) Touch(key string, exptime int64) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	item, ok := shard.items[key]
	if !ok || isExpired(item) {
		return false
	}
	if exptime > 0 {
		item.exptime = time.Now().Unix() + exptime
	} else {
		item.exptime = 0
	}
	return true
}

// Incr increments a numeric value.
func (s *Store) Incr(key string, delta uint64) (uint64, bool, string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	item, ok := shard.items[key]
	if !ok || isExpired(item) {
		return 0, false, "NOT_FOUND\r\n"
	}
	current, err := strconv.ParseUint(string(item.value), 10, 64)
	if err != nil {
		return 0, false, "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
	}
	newVal := current + delta
	item.value = []byte(strconv.FormatUint(newVal, 10))
	item.casUnique = atomic.AddInt64(&casCounter, 1)
	return newVal, true, strconv.FormatUint(newVal, 10) + "\r\n"
}

// Decr decrements a numeric value.
func (s *Store) Decr(key string, delta uint64) (uint64, bool, string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	item, ok := shard.items[key]
	if !ok || isExpired(item) {
		return 0, false, "NOT_FOUND\r\n"
	}
	current, err := strconv.ParseUint(string(item.value), 10, 64)
	if err != nil {
		return 0, false, "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
	}
	var newVal uint64
	if current < delta {
		newVal = 0
	} else {
		newVal = current - delta
	}
	item.value = []byte(strconv.FormatUint(newVal, 10))
	item.casUnique = atomic.AddInt64(&casCounter, 1)
	return newVal, true, strconv.FormatUint(newVal, 10) + "\r\n"
}

// FlushAll deletes all items. If delay > 0, the flush is delayed.
func (s *Store) FlushAll(delay int) string {
	if delay > 0 {
		go func() {
			time.Sleep(time.Duration(delay) * time.Second)
			s.flushAllImmediate()
		}()
	} else {
		s.flushAllImmediate()
	}
	return "OK\r\n"
}

func (s *Store) flushAllImmediate() {
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.items = make(map[string]*Item)
		shard.mu.Unlock()
	}
}

// Stats returns a minimal set of server statistics.
func (s *Store) Stats() string {
	var totalItems int
	for _, shard := range s.shards {
		shard.mu.RLock()
		totalItems += len(shard.items)
		shard.mu.RUnlock()
	}
	stats := "STAT curr_items " + strconv.Itoa(totalItems) + "\r\n"
	stats += "STAT total_shards " + strconv.Itoa(s.count) + "\r\n"
	stats += fmt.Sprintf("STAT version %s\r\n", version.GetVersion())
	stats += "END\r\n"
	return stats
}

// setVerbosity sets the server's verbosity level.
func setVerbosity(_level int) string {
	// TODO: Implement verbosity level handling.
	// verbosityLevel = level
	return "OK\r\n"
}

// Save saves the current state of the store to disk as JSON.
func (s *Store) Save(path string) {
	path, err := expandPath(path)
	if err != nil {
		fmt.Printf("Error expanding path: %v\n", err)
		return
	}
	// 構造体を用いて各アイテムの情報をまとめる
	var dataToSave []PersistItem

	// 各シャードからアイテムを収集する
	for _, shard := range s.shards {
		shard.mu.RLock()
		for key, item := range shard.items {
			// 保存時点で期限切れのアイテムは保存しない
			if isExpired(item) {
				continue
			}
			dataToSave = append(dataToSave, PersistItem{
				Key:       key,
				Value:     item.value,
				Flags:     item.flags,
				Exptime:   item.exptime,
				CasUnique: item.casUnique,
			})
		}
		shard.mu.RUnlock()
	}

	// JSON に変換する
	jsonData, err := json.MarshalIndent(dataToSave, "", "  ")
	if err != nil {
		fmt.Printf("Error marshalling store: %v\n", err)
		return
	}

	// ファイルに書き出す
	err = os.WriteFile(path, jsonData, 0644)
	if err != nil {
		fmt.Printf("Error writing store to file: %v\n", err)
	}
}

// Load loads the store state from disk.
func (s *Store) Load(path string) {
	path, err := expandPath(path)
	if err != nil {
		fmt.Printf("Error expanding path: %v\n", err)
		return
	}
	// ファイルからデータを読み込む
	jsonData, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Error reading store from file: %v\n", err)
		return
	}

	var loadedItems []PersistItem
	err = json.Unmarshal(jsonData, &loadedItems)
	if err != nil {
		fmt.Printf("Error unmarshalling store: %v\n", err)
		return
	}

	// 現在のストアをクリアする
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.items = make(map[string]*Item)
		shard.mu.Unlock()
	}

	// casCounter の更新用
	var maxCas int64

	// 読み込んだアイテムを各シャードに追加する
	for _, pi := range loadedItems {
		shard := s.getShard(pi.Key)
		shard.mu.Lock()
		shard.items[pi.Key] = &Item{
			value:     pi.Value,
			flags:     pi.Flags,
			exptime:   pi.Exptime,
			casUnique: pi.CasUnique,
		}
		shard.mu.Unlock()
		if pi.CasUnique > maxCas {
			maxCas = pi.CasUnique
		}
	}

	// グローバルな casCounter を最新の値に更新する
	atomic.StoreInt64(&casCounter, maxCas)
}

// connContext holds per-connection state.
type ConnContext struct {
	buffer           []byte   // Received data buffer.
	expectingData    bool     // Waiting for a data block for a storage command.
	expectedBytes    int      // Expected bytes count (including CRLF).
	pendingSetTokens []string // Storage command tokens.
}

func NewConnContext(size int) *ConnContext {
	return &ConnContext{
		buffer:           make([]byte, 0, size),
		expectingData:    false,
		expectedBytes:    0,
		pendingSetTokens: nil,
	}
}

func (c *ConnContext) GetBuffer() []byte {
	return c.buffer
}

func (c *ConnContext) SetBuffer(data []byte) {
	c.buffer = data
}

// processCommands processes incoming data and returns response bytes.
func (s *Store) ProcessCommands(ctx *ConnContext) ([]byte, bool) {
	var response bytes.Buffer
	for {
		if !ctx.expectingData {
			// Look for a complete command line.
			idx := bytes.IndexByte(ctx.buffer, '\n')
			if idx == -1 {
				break // Incomplete line.
			}
			line := ctx.buffer[:idx]
			ctx.buffer = ctx.buffer[idx+1:]
			line = bytes.TrimRight(line, "\r")
			if len(line) == 0 {
				continue
			}
			tokens := strings.Fields(string(line))
			if len(tokens) == 0 {
				continue
			}
			cmd := tokens[0]
			// Commands that require a following data block.
			if cmd == "save" || cmd == "load" {
				if len(tokens) != 2 {
					response.WriteString("CLIENT_ERROR bad command line format\r\n")
					continue
				}
				if cmd == "save" {
					s.Save(tokens[1])
					response.WriteString("END")
				} else if cmd == "load" {
					s.Load(tokens[1])
					response.WriteString("END")
				}
			} else if cmd == "set" || cmd == "add" || cmd == "replace" || cmd == "append" || cmd == "prepend" || cmd == "cas" {
				required := 5
				if cmd == "cas" {
					required = 6
				}
				if len(tokens) < required {
					response.WriteString("CLIENT_ERROR bad command line format\r\n")
					continue
				}
				bytesNum, err := strconv.Atoi(tokens[4])
				if err != nil {
					response.WriteString("CLIENT_ERROR bad command line format\r\n")
					continue
				}
				ctx.expectingData = true
				ctx.expectedBytes = bytesNum + 2 // data block plus CRLF.
				ctx.pendingSetTokens = tokens
				continue
			} else {
				// Non-storage commands.
				if cmd == "quit" {
					// quit: close connection. Here, no response is returned and the loop is terminated.
					return []byte("quit"), true
				}
				resp := s.handleNonSetCommand(tokens)
				response.WriteString(resp)
			}
		} else {
			// In storage command data block mode.
			if len(ctx.buffer) < ctx.expectedBytes {
				break // Data block not complete.
			}
			dataBlock := ctx.buffer[:ctx.expectedBytes]
			ctx.buffer = ctx.buffer[ctx.expectedBytes:]
			// Check: The data block must end with CRLF.
			if dataBlock[len(dataBlock)-2] != '\r' || dataBlock[len(dataBlock)-1] != '\n' {
				response.WriteString("CLIENT_ERROR bad data chunk\r\n")
				ctx.expectingData = false
				ctx.pendingSetTokens = nil
				continue
			}
			// Remove CRLF to extract the value portion.
			data := dataBlock[:len(dataBlock)-2]
			resp := s.handleSetCommand(ctx.pendingSetTokens, data)
			response.WriteString(resp)
			ctx.expectingData = false
			ctx.pendingSetTokens = nil
		}
	}
	return response.Bytes(), false
}

// handleSetCommand processes storage commands (set, add, replace, append, prepend, cas) that include a data block.
func (s *Store) handleSetCommand(tokens []string, data []byte) string {
	cmd := tokens[0]
	if cmd == "set" || cmd == "add" || cmd == "replace" {
		if len(tokens) < 5 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		key := tokens[1]
		flags, err1 := strconv.Atoi(tokens[2])
		exptime, err2 := strconv.ParseInt(tokens[3], 10, 64)
		bytesNum, err3 := strconv.Atoi(tokens[4])
		if err1 != nil || err2 != nil || err3 != nil {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		if len(data) != bytesNum {
			return "CLIENT_ERROR data block size mismatch\r\n"
		}
		if cmd == "set" {
			s.Set(key, flags, exptime, data)
			return "STORED\r\n"
		} else if cmd == "add" {
			if s.Add(key, flags, exptime, data) {
				return "STORED\r\n"
			}
			return "NOT_STORED\r\n"
		} else if cmd == "replace" {
			if s.Replace(key, flags, exptime, data) {
				return "STORED\r\n"
			}
			return "NOT_STORED\r\n"
		}
	} else if cmd == "append" || cmd == "prepend" {
		if len(tokens) < 5 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		key := tokens[1]
		bytesNum, err := strconv.Atoi(tokens[4])
		if err != nil {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		if len(data) != bytesNum {
			return "CLIENT_ERROR data block size mismatch\r\n"
		}
		if cmd == "append" {
			if s.Append(key, data) {
				return "STORED\r\n"
			}
			return "NOT_STORED\r\n"
		} else { // prepend
			if s.Prepend(key, data) {
				return "STORED\r\n"
			}
			return "NOT_STORED\r\n"
		}
	} else if cmd == "cas" {
		if len(tokens) < 6 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		key := tokens[1]
		flags, err1 := strconv.Atoi(tokens[2])
		exptime, err2 := strconv.ParseInt(tokens[3], 10, 64)
		bytesNum, err3 := strconv.Atoi(tokens[4])
		casUnique, err4 := strconv.ParseInt(tokens[5], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		if len(data) != bytesNum {
			return "CLIENT_ERROR data block size mismatch\r\n"
		}
		return s.Cas(key, flags, exptime, data, casUnique)
	}
	return "ERROR\r\n"
}

// handleNonSetCommand processes commands that do not require a data block.
func (s *Store) handleNonSetCommand(tokens []string) string {
	cmd := tokens[0]
	switch cmd {
	case "get":
		if len(tokens) < 2 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		var sb strings.Builder
		now := time.Now().Unix()
		for i := 1; i < len(tokens); i++ {
			key := tokens[i]
			if item, ok := s.Get(key); ok && (item.exptime == 0 || now <= item.exptime) {
				sb.WriteString("VALUE " + key + " " + strconv.Itoa(item.flags) + " " + strconv.Itoa(len(item.value)) + "\r\n")
				sb.Write(item.value)
				sb.WriteString("\r\n")
			}
		}
		sb.WriteString("END\r\n")
		return sb.String()
	case "gets":
		if len(tokens) < 2 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		var sb strings.Builder
		now := time.Now().Unix()
		for i := 1; i < len(tokens); i++ {
			key := tokens[i]
			shard := s.getShard(key)
			shard.mu.RLock()
			item, ok := shard.items[key]
			if ok && (item.exptime == 0 || now <= item.exptime) {
				sb.WriteString("VALUE " + key + " " + strconv.Itoa(item.flags) + " " + strconv.Itoa(len(item.value)) + " " + strconv.FormatInt(item.casUnique, 10) + "\r\n")
				sb.Write(item.value)
				sb.WriteString("\r\n")
			}
			shard.mu.RUnlock()
		}
		sb.WriteString("END\r\n")
		return sb.String()
	case "delete":
		if len(tokens) < 2 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		key := tokens[1]
		if s.Delete(key) {
			return "DELETED\r\n"
		}
		return "NOT_FOUND\r\n"
	case "flush_all":
		delay := 0
		if len(tokens) >= 2 {
			d, err := strconv.Atoi(tokens[1])
			if err == nil {
				delay = d
			}
		}
		return s.FlushAll(delay)
	case "incr":
		if len(tokens) < 3 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		key := tokens[1]
		delta, err := strconv.ParseUint(tokens[2], 10, 64)
		if err != nil {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		_, _, resp := s.Incr(key, delta)
		return resp
	case "decr":
		if len(tokens) < 3 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		key := tokens[1]
		delta, err := strconv.ParseUint(tokens[2], 10, 64)
		if err != nil {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		_, _, resp := s.Decr(key, delta)
		return resp
	case "touch":
		if len(tokens) < 3 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		key := tokens[1]
		exptime, err := strconv.Atoi(tokens[2])
		if err != nil {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		if s.Touch(key, int64(exptime)) {
			return "TOUCHED\r\n"
		}
		return "NOT_FOUND\r\n"
	case "stats":
		return s.Stats()
	case "version":
		return fmt.Sprintf("VERSION %s\r\n", version.GetVersion())
	case "verbosity":
		if len(tokens) < 2 {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		level, err := strconv.Atoi(tokens[1])
		if err != nil {
			return "CLIENT_ERROR bad command line format\r\n"
		}
		return setVerbosity(level)
	default:
		return "ERROR\r\n"
	}
}
