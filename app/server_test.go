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

	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.Nil(t, err)

	buf = make([]byte, 1024)
	n, err = conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "+PONG\r\n", string(buf[:n]))
}

func Test_Ping2Connections(t *testing.T) {

	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)

	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.Nil(t, err)

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "+PONG\r\n", string(buf[:n]))

	conn2, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)

	_, err = conn2.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.Nil(t, err)

	buf = make([]byte, 1024)
	n, err = conn2.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "+PONG\r\n", string(buf[:n]))

	// use the first connection again

	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.Nil(t, err)

	buf = make([]byte, 1024)
	n, err = conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "+PONG\r\n", string(buf[:n]))

}

func Test_Echo(t *testing.T) {

	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)

	_, err = conn.Write([]byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"))
	assert.Nil(t, err)

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "$3\r\nhey\r\n", string(buf[:n]))
}

func Test_SetGet(t *testing.T) {

	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)

	_, err = conn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	assert.Nil(t, err)

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "+OK\r\n", string(buf[:n]))

	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	assert.Nil(t, err)

	buf = make([]byte, 1024)
	n, err = conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "$3\r\nbar\r\n", string(buf[:n]))
}

func TestWithExpiration(t *testing.T) {

	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)

	_, err = conn.Write([]byte("*5\r\n$3\r\nSET\r\n$3\r\nfoe\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n"))
	assert.Nil(t, err)

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)

	assert.Nil(t, err)
	assert.Equal(t, "+OK\r\n", string(buf[:n]))

	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoe\r\n"))
	assert.Nil(t, err)

	buf = make([]byte, 1024)
	n, err = conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "$3\r\nbar\r\n", string(buf[:n]))

	time.Sleep(200 * time.Millisecond)

	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoe\r\n"))
	assert.Nil(t, err)

	buf = make([]byte, 1024)
	n, err = conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "$-1\r\n", string(buf[:n]))
}

func TestInfo(t *testing.T) {

	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)

	_, err = conn.Write([]byte("*1\r\n$4\r\nINFO\r\n"))
	assert.Nil(t, err)

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, "$11\r\nReplication\r\nrole:master\r\n", string(buf[:n]))
}

func TestInfoSlave(t *testing.T) {

	s := NewServer("0.0.0.0:6389")

	info := s.getInfo()
	assert.Equal(t, "Replication", info[0])
	assert.Equal(t, "role:master", info[1])

	s.AsSlaveOf("0.0.0.0:6379")

	info = s.getInfo()

	assert.Equal(t, "Replication", info[0])
	assert.Equal(t, "role:slave", info[1])
}
