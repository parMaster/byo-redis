package main

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Ping(t *testing.T) {

	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)

	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.Nil(t, err)

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "+PONG\r\n", string(buf[:n]))
}

// MockConn is a mock implementation of the net.Conn interface
type MockConn struct {
	cmd string
}

// Close is a mock implementation of the net.Conn Close method
func (m *MockConn) Close() error {
	return nil
}

// LocalAddr is a mock implementation of the net.Conn LocalAddr method
func (m *MockConn) LocalAddr() net.Addr {
	return nil
}

// RemoteAddr is a mock implementation of the net.Conn RemoteAddr method
func (m *MockConn) RemoteAddr() net.Addr {
	return nil
}

// SetDeadline is a mock implementation of the net.Conn SetDeadline method
func (m *MockConn) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline is a mock implementation of the net.Conn SetReadDeadline method
func (m *MockConn) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline is a mock implementation of the net.Conn SetWriteDeadline method
func (m *MockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// Write is a mock implementation of the net.Conn Write method
func (m *MockConn) Write(b []byte) (n int, err error) {
	m.cmd = string(b)
	return len(b), nil
}

// Read is a mock implementation of the net.Conn Read method
func (m *MockConn) Read(b []byte) (n int, err error) {
	if len(m.cmd) == 0 {
		return 0, nil
	}
	copy(b, []byte(m.cmd))
	read := len(m.cmd)
	return read, nil
}

func Test_handleConnection(t *testing.T) {

	mockConn := &MockConn{}

	cmd := "*1\r\n$4\r\nPING\r\n"
	wbytes, err := mockConn.Write([]byte(cmd))
	assert.Nil(t, err)
	assert.Equal(t, len(cmd), wbytes)

	assert.Equal(t, "*1\r\n$4\r\nPING\r\n", mockConn.cmd)

	handleConnection(mockConn)

	buf := make([]byte, 1024)
	n, err := mockConn.Read(buf)
	assert.Nil(t, err)
	assert.Equal(t, "+PONG\r\n", string(buf[:n]))
}
