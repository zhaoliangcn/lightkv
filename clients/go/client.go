// Package lightkv provides a client SDK for LightKV server.
// It connects via TCP using Redis RESP protocol with zero external dependencies.
package lightkv

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is a LightKV client that communicates with the server via TCP
// using the Redis RESP protocol.
type Client struct {
	addr      string
	timeout   time.Duration
	conn      net.Conn
	reader    *bufio.Reader
	mu        sync.Mutex
	lastError string

	// Pipeline support
	pipelineBuf []string
	pipelineLen int
}

// NewClient creates a new LightKV client.
func NewClient(addr string, timeout time.Duration) *Client {
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		addr:    addr,
		timeout: timeout,
	}
}

// Connect establishes a connection to the LightKV server.
func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		c.lastError = err.Error()
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.lastError = ""
	return nil
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.reader = nil
		return err
	}
	return nil
}

// IsConnected returns whether the client is connected.
func (c *Client) IsConnected() bool {
	return c.conn != nil
}

// GetLastError returns the last error message.
func (c *Client) GetLastError() string {
	return c.lastError
}

// ─── Pipeline Support ───

// Pipeline begins buffering commands. Call ExecPipeline to send them all at once.
func (c *Client) Pipeline() {
	c.pipelineBuf = nil
	c.pipelineLen = 0
}

// Queue adds a command to the pipeline buffer.
func (c *Client) Queue(args []string) {
	c.pipelineBuf = append(c.pipelineBuf, c.buildRespString(args))
	c.pipelineLen += len(c.buildRespString(args))
}

// ExecPipeline sends all queued commands and returns their responses.
func (c *Client) ExecPipeline() ([]any, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("not connected")
	}
	if len(c.pipelineBuf) == 0 {
		return nil, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Send all commands in one write
	var buf strings.Builder
	for _, cmd := range c.pipelineBuf {
		buf.WriteString(cmd)
	}
	_, err := c.conn.Write([]byte(buf.String()))
	if err != nil {
		c.lastError = err.Error()
		c.pipelineBuf = nil
		c.pipelineLen = 0
		return nil, err
	}

	// Read all responses
	count := len(c.pipelineBuf)
	results := make([]any, count)
	for i := 0; i < count; i++ {
		resp, err := c.readResponse()
		if err != nil {
			c.pipelineBuf = nil
			c.pipelineLen = 0
			return results[:i], err
		}
		results[i] = resp
	}

	c.pipelineBuf = nil
	c.pipelineLen = 0
	return results, nil
}

func (c *Client) buildRespString(args []string) string {
	var buf strings.Builder
	buf.WriteByte('*')
	buf.WriteString(strconv.Itoa(len(args)))
	buf.WriteString("\r\n")
	for _, arg := range args {
		buf.WriteByte('$')
		buf.WriteString(strconv.Itoa(len(arg)))
		buf.WriteString("\r\n")
		buf.WriteString(arg)
		buf.WriteString("\r\n")
	}
	return buf.String()
}

// sendCommand sends a RESP command and returns the parsed response.
func (c *Client) sendCommand(args []string) (any, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	cmd := c.buildRespString(args)

	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.conn.Write([]byte(cmd))
	if err != nil {
		c.lastError = err.Error()
		return nil, err
	}

	return c.readResponse()
}

// readResponse reads and parses a RESP response.
func (c *Client) readResponse() (any, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 {
		return nil, fmt.Errorf("invalid response")
	}

	respType := line[0]
	content := line[1 : len(line)-2] // strip type byte and \r\n

	switch respType {
	case '+':
		return content, nil

	case '-':
		c.lastError = content
		return nil, fmt.Errorf("server error: %s", content)

	case ':':
		val, err := strconv.ParseInt(content, 10, 64)
		if err != nil {
			return nil, err
		}
		return val, nil

	case '$':
		length, err := strconv.ParseInt(content, 10, 64)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, nil // nil
		}
		data := make([]byte, length)
		_, err = c.reader.Read(data)
		if err != nil {
			return nil, err
		}
		trailer := make([]byte, 2)
		_, err = c.reader.Read(trailer)
		if err != nil {
			return nil, err
		}
		return string(data), nil

	case '*':
		count, err := strconv.ParseInt(content, 10, 64)
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, nil // nil array
		}
		result := make([]any, count)
		for i := int64(0); i < count; i++ {
			elem, err := c.readResponse()
			if err != nil {
				return nil, err
			}
			result[i] = elem
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unknown response type: %c", respType)
	}
}

// Set sets a key-value pair.
func (c *Client) Set(key, value string) (bool, error) {
	resp, err := c.sendCommand([]string{"SET", key, value})
	if err != nil {
		return false, err
	}
	return resp == "OK", nil
}

// Get retrieves the value for a key. Returns (value, true) if found,
// or ("", false) if the key does not exist.
func (c *Client) Get(key string) (string, bool, error) {
	resp, err := c.sendCommand([]string{"GET", key})
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, nil
	}
	return resp.(string), true, nil
}

// Delete removes a key.
func (c *Client) Delete(key string) (bool, error) {
	resp, err := c.sendCommand([]string{"DEL", key})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// DeleteRange removes all keys in the range [begin, end).
func (c *Client) DeleteRange(begin, end string) (bool, error) {
	resp, err := c.sendCommand([]string{"DELRANGE", begin, end})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// Ping sends a ping to the server.
func (c *Client) Ping() (bool, error) {
	resp, err := c.sendCommand([]string{"PING"})
	if err != nil {
		return false, err
	}
	return resp == "PONG", nil
}

// Stats retrieves server statistics.
func (c *Client) Stats() (map[string]string, error) {
	resp, err := c.sendCommand([]string{"STATS"})
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return map[string]string{}, nil
	}
	result := make(map[string]string)
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		v, _ := arr[i+1].(string)
		result[k] = v
	}
	return result, nil
}

// Quit sends a quit command and closes the connection.
func (c *Client) Quit() error {
	_, _ = c.sendCommand([]string{"QUIT"})
	return c.Close()
}

// ─── String Extension Commands ───

// Incr increments a key by 1.
func (c *Client) Incr(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"INCR", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// Decr decrements a key by 1.
func (c *Client) Decr(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"DECR", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// IncrBy increments a key by delta.
func (c *Client) IncrBy(key string, delta int64) (int64, error) {
	resp, err := c.sendCommand([]string{"INCRBY", key, strconv.FormatInt(delta, 10)})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// DecrBy decrements a key by delta.
func (c *Client) DecrBy(key string, delta int64) (int64, error) {
	resp, err := c.sendCommand([]string{"DECRBY", key, strconv.FormatInt(delta, 10)})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// IncrByFloat increments a key by a float delta.
func (c *Client) IncrByFloat(key string, delta float64) (string, error) {
	resp, err := c.sendCommand([]string{"INCRBYFLOAT", key, strconv.FormatFloat(delta, 'f', -1, 64)})
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

// MSet sets multiple key-value pairs.
func (c *Client) MSet(kvs [][2]string) error {
	args := []string{"MSET"}
	for _, kv := range kvs {
		args = append(args, kv[0], kv[1])
	}
	_, err := c.sendCommand(args)
	return err
}

// MGet retrieves values for multiple keys.
func (c *Client) MGet(keys []string) ([]any, error) {
	args := []string{"MGET"}
	args = append(args, keys...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.([]any), nil
}

// SetEx sets a key with expiration in seconds.
func (c *Client) SetEx(key string, seconds int64, value string) error {
	resp, err := c.sendCommand([]string{"SETEX", key, strconv.FormatInt(seconds, 10), value})
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("SETEX failed: %v", resp)
	}
	return nil
}

// SetNx sets a key only if it does not exist.
func (c *Client) SetNx(key, value string) (bool, error) {
	resp, err := c.sendCommand([]string{"SETNX", key, value})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// GetSet sets a key and returns the old value.
func (c *Client) GetSet(key, value string) (any, error) {
	return c.sendCommand([]string{"GETSET", key, value})
}

// Append appends a value to a key's current value.
func (c *Client) Append(key, value string) (int64, error) {
	resp, err := c.sendCommand([]string{"APPEND", key, value})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// StrLen returns the string length of a key's value.
func (c *Client) StrLen(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"STRLEN", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// ─── General Commands ───

// Exists checks if one or more keys exist.
func (c *Client) Exists(keys []string) (int64, error) {
	args := []string{"EXISTS"}
	args = append(args, keys...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// Expire sets a key's expiration in seconds.
func (c *Client) Expire(key string, seconds int64) (bool, error) {
	resp, err := c.sendCommand([]string{"EXPIRE", key, strconv.FormatInt(seconds, 10)})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// Ttl returns the time-to-live of a key in seconds.
func (c *Client) Ttl(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"TTL", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// Pttl returns the time-to-live of a key in milliseconds.
func (c *Client) Pttl(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"PTTL", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// Persist removes the expiration from a key.
func (c *Client) Persist(key string) (bool, error) {
	resp, err := c.sendCommand([]string{"PERSIST", key})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// Type returns the data type of a key.
func (c *Client) Type(key string) (string, error) {
	resp, err := c.sendCommand([]string{"TYPE", key})
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

// Rename renames a key.
func (c *Client) Rename(key, newKey string) error {
	resp, err := c.sendCommand([]string{"RENAME", key, newKey})
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("RENAME failed: %v", resp)
	}
	return nil
}

// RenameNx renames a key only if the new key does not exist.
func (c *Client) RenameNx(key, newKey string) (bool, error) {
	resp, err := c.sendCommand([]string{"RENAMENX", key, newKey})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// Keys returns all keys matching a pattern.
func (c *Client) Keys(pattern string) ([]string, error) {
	resp, err := c.sendCommand([]string{"KEYS", pattern})
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return nil, nil
	}
	result := make([]string, len(arr))
	for i, v := range arr {
		result[i], _ = v.(string)
	}
	return result, nil
}

// ─── Hash Commands ───

// HSet sets one or more hash fields. Returns number of new fields added.
func (c *Client) HSet(key string, fields [][2]string) (int64, error) {
	args := []string{"HSET", key}
	for _, f := range fields {
		args = append(args, f[0], f[1])
	}
	resp, err := c.sendCommand(args)
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// HGet retrieves the value of a hash field.
func (c *Client) HGet(key, field string) (string, bool, error) {
	resp, err := c.sendCommand([]string{"HGET", key, field})
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, nil
	}
	return resp.(string), true, nil
}

// HMSet sets multiple hash fields.
func (c *Client) HMSet(key string, fields [][2]string) error {
	args := []string{"HMSET", key}
	for _, f := range fields {
		args = append(args, f[0], f[1])
	}
	resp, err := c.sendCommand(args)
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("HMSET failed: %v", resp)
	}
	return nil
}

// HMGet retrieves values for multiple hash fields.
func (c *Client) HMGet(key string, fields []string) ([]any, error) {
	args := []string{"HMGET", key}
	args = append(args, fields...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.([]any), nil
}

// HGetAll retrieves all fields and values of a hash.
func (c *Client) HGetAll(key string) (map[string]string, error) {
	resp, err := c.sendCommand([]string{"HGETALL", key})
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return map[string]string{}, nil
	}
	result := make(map[string]string)
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		v, _ := arr[i+1].(string)
		result[k] = v
	}
	return result, nil
}

// HDel deletes one or more hash fields. Returns number of deleted fields.
func (c *Client) HDel(key string, fields []string) (int64, error) {
	args := []string{"HDEL", key}
	args = append(args, fields...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// HExists checks if a hash field exists.
func (c *Client) HExists(key, field string) (bool, error) {
	resp, err := c.sendCommand([]string{"HEXISTS", key, field})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// HLen returns the number of fields in a hash.
func (c *Client) HLen(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"HLEN", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// HKeys returns all field names in a hash.
func (c *Client) HKeys(key string) ([]string, error) {
	resp, err := c.sendCommand([]string{"HKEYS", key})
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return nil, nil
	}
	result := make([]string, len(arr))
	for i, v := range arr {
		result[i], _ = v.(string)
	}
	return result, nil
}

// HVals returns all field values in a hash.
func (c *Client) HVals(key string) ([]string, error) {
	resp, err := c.sendCommand([]string{"HVALS", key})
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return nil, nil
	}
	result := make([]string, len(arr))
	for i, v := range arr {
		result[i], _ = v.(string)
	}
	return result, nil
}

// HIncrBy increments a hash field by an integer delta.
func (c *Client) HIncrBy(key, field string, delta int64) (int64, error) {
	resp, err := c.sendCommand([]string{"HINCRBY", key, field, strconv.FormatInt(delta, 10)})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// HStrLen returns the string length of a hash field value.
func (c *Client) HStrLen(key, field string) (int64, error) {
	resp, err := c.sendCommand([]string{"HSTRLEN", key, field})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// ─── List Commands ───

// LPush prepends values to a list. Returns list length.
func (c *Client) LPush(key string, values []string) (int64, error) {
	args := []string{"LPUSH", key}
	args = append(args, values...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// RPush appends values to a list. Returns list length.
func (c *Client) RPush(key string, values []string) (int64, error) {
	args := []string{"RPUSH", key}
	args = append(args, values...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// LPop removes and returns the first element of a list.
func (c *Client) LPop(key string) (string, bool, error) {
	resp, err := c.sendCommand([]string{"LPOP", key})
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, nil
	}
	return resp.(string), true, nil
}

// RPop removes and returns the last element of a list.
func (c *Client) RPop(key string) (string, bool, error) {
	resp, err := c.sendCommand([]string{"RPOP", key})
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, nil
	}
	return resp.(string), true, nil
}

// LRange returns a range of elements from a list.
func (c *Client) LRange(key string, start, stop int64) ([]string, error) {
	resp, err := c.sendCommand([]string{"LRANGE", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10)})
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return nil, nil
	}
	result := make([]string, len(arr))
	for i, v := range arr {
		result[i], _ = v.(string)
	}
	return result, nil
}

// LLen returns the length of a list.
func (c *Client) LLen(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"LLEN", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// LIndex returns the element at index in a list.
func (c *Client) LIndex(key string, idx int64) (string, bool, error) {
	resp, err := c.sendCommand([]string{"LINDEX", key, strconv.FormatInt(idx, 10)})
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, nil
	}
	return resp.(string), true, nil
}

// LSet sets the value of an element at index in a list.
func (c *Client) LSet(key string, idx int64, value string) error {
	resp, err := c.sendCommand([]string{"LSET", key, strconv.FormatInt(idx, 10), value})
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("LSET failed: %v", resp)
	}
	return nil
}

// LTrim trims a list to the specified range.
func (c *Client) LTrim(key string, start, stop int64) error {
	resp, err := c.sendCommand([]string{"LTRIM", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10)})
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("LTRIM failed: %v", resp)
	}
	return nil
}

// LRem removes elements from a list. Returns number of removed elements.
func (c *Client) LRem(key string, count int64, value string) (int64, error) {
	resp, err := c.sendCommand([]string{"LREM", key, strconv.FormatInt(count, 10), value})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// ─── Set Commands ───

// SAdd adds members to a set. Returns number of added members.
func (c *Client) SAdd(key string, members []string) (int64, error) {
	args := []string{"SADD", key}
	args = append(args, members...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// SRem removes members from a set. Returns number of removed members.
func (c *Client) SRem(key string, members []string) (int64, error) {
	args := []string{"SREM", key}
	args = append(args, members...)
	resp, err := c.sendCommand(args)
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// SMembers returns all members of a set.
func (c *Client) SMembers(key string) ([]string, error) {
	resp, err := c.sendCommand([]string{"SMEMBERS", key})
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return nil, nil
	}
	result := make([]string, len(arr))
	for i, v := range arr {
		result[i], _ = v.(string)
	}
	return result, nil
}

// SIsMember checks if a member is in a set.
func (c *Client) SIsMember(key, member string) (bool, error) {
	resp, err := c.sendCommand([]string{"SISMEMBER", key, member})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

// SCard returns the number of members in a set.
func (c *Client) SCard(key string) (int64, error) {
	resp, err := c.sendCommand([]string{"SCARD", key})
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// SPop removes and returns a random member from a set.
func (c *Client) SPop(key string) (string, bool, error) {
	resp, err := c.sendCommand([]string{"SPOP", key})
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, nil
	}
	return resp.(string), true, nil
}

// SRandMember returns a random member from a set without removing it.
func (c *Client) SRandMember(key string) (string, bool, error) {
	resp, err := c.sendCommand([]string{"SRANDMEMBER", key})
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, nil
	}
	return resp.(string), true, nil
}

// SMove moves a member from source set to destination set.
func (c *Client) SMove(src, dst, member string) (bool, error) {
	resp, err := c.sendCommand([]string{"SMOVE", src, dst, member})
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}
