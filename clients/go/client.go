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

// sendCommand sends a RESP command and returns the parsed response.
func (c *Client) sendCommand(args []string) (any, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	// Build RESP command
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

	c.mu.Lock()
	defer c.mu.Unlock()

	// Send command
	_, err := c.conn.Write([]byte(buf.String()))
	if err != nil {
		c.lastError = err.Error()
		return nil, err
	}

	// Read response
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
		// Simple string
		return content, nil

	case '-':
		// Error
		c.lastError = content
		return nil, fmt.Errorf("server error: %s", content)

	case ':':
		// Integer
		val, err := strconv.ParseInt(content, 10, 64)
		if err != nil {
			return nil, err
		}
		return val, nil

	case '$':
		// Bulk string
		length, err := strconv.ParseInt(content, 10, 64)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, nil // nil
		}
		// Read the actual data
		data := make([]byte, length)
		_, err = c.reader.Read(data)
		if err != nil {
			return nil, err
		}
		// Read trailing \r\n
		trailer := make([]byte, 2)
		_, err = c.reader.Read(trailer)
		if err != nil {
			return nil, err
		}
		return string(data), nil

	case '*':
		// Array
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
