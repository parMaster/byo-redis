package main

import (
	"bufio"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var s *Server

// Run the server in a goroutine
func init() {
	go func() {
		s = NewServer("0.0.0.0:6379")
		s.ListenAndServe()
	}()
}

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

	info := string(buf[:n])
	infos := strings.Split(info, "\r\n")
	assert.Equal(t, "Replication", infos[1])
	assert.Equal(t, "role:master", infos[2])
	assert.Equal(t, "master_replid:", infos[3][:14])

}

func TestReplicas(t *testing.T) {

	r1addr := "127.0.0.1:6395"
	r1 := NewServer(r1addr)
	//
	go r1.ListenAndServe()
	err := r1.AsSlaveOf("0.0.0.0:6379")
	assert.Nil(t, err)

	r2addr := "127.0.0.1:6396"
	r2 := NewServer(r2addr)
	//
	go r2.ListenAndServe()
	err = r2.AsSlaveOf("0.0.0.0:6379")
	assert.Nil(t, err)

	time.Sleep(time.Second)

	// check the number of replicas
	assert.Equal(t, 2, len(s.replicas))

	// check the replicas addresses and ports
	assert.Equal(t, "127.0.0.1", s.replicas[r1addr].Addr)
	assert.Equal(t, 6395, s.replicas[r1addr].Port)

	assert.Equal(t, "127.0.0.1", s.replicas[r2addr].Addr)
	assert.Equal(t, 6396, s.replicas[r2addr].Port)

	// try to set a key in the master
	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	assert.Nil(t, err)
	// buf := make([]byte, 1024)
	reader := bufio.NewReader(conn)
	_, err = conn.Write([]byte("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n$2\r\npx\r\n$4\r\n5000\r\n"))
	reader.ReadLine()
	assert.Nil(t, err)
	_, err = conn.Write([]byte("*5\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n$2\r\npx\r\n$4\r\n5000\r\n"))
	reader.ReadLine()
	assert.Nil(t, err)
	_, err = conn.Write([]byte("*5\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n$2\r\npx\r\n$4\r\n5000\r\n"))
	assert.Nil(t, err)
	reader.ReadLine()

	time.Sleep(time.Second)

	v1, err := r1.storage.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, "123", v1)

	v1, err = r1.storage.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, "456", v1)

	v1, err = r1.storage.Get("baz")
	assert.Nil(t, err)
	assert.Equal(t, "789", v1)

	v2, err := r2.storage.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, "123", v2)

	v2, err = r2.storage.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, "456", v2)

	v2, err = r2.storage.Get("baz")
	assert.Nil(t, err)
	assert.Equal(t, "789", v2)

}

//
// Testing functions separately
//

func TestInfoSlave(t *testing.T) {

	s := NewServer("0.0.0.0:6389")

	info := s.getInfo()
	assert.Equal(t, "Replication", info[0])
	assert.Equal(t, "role:master", info[1])
	masterReplId := info[2]
	log.Println(masterReplId)
	assert.Equal(t, "master_replid:", info[2][:14])
	assert.Equal(t, 14+40, len(masterReplId))
	assert.Equal(t, "master_repl_offset:0", info[3])

	s.AsSlaveOf("0.0.0.0:6379")

	info = s.getInfo()

	assert.Equal(t, "Replication", info[0])
	assert.Equal(t, "role:slave", info[1])
}

func TestReplConf(t *testing.T) {

	s := NewServer("0.0.0.0:6389")

	// wrong number of arguments
	err := s.replConf("remote:6390", []string{"REPLCONF", "capa", "eof", "capa"})
	assert.Error(t, err)

	// REPLCONF capa eof capa psync2
	err = s.replConf("remote:6390", []string{"REPLCONF", "capa", "eof", "capa", "psync2"})
	assert.Nil(t, err)

	assert.Contains(t, s.replicas["remote:6390"].capabilities, "eof")
	assert.Contains(t, s.replicas["remote:6390"].capabilities, "psync2")

	// PSYNC ? -1
	err = s.psyncConfig([]string{"PSYNC", "?", "-1"})
	assert.Nil(t, err)
}
