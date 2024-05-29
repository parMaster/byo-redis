package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/parMaster/mcache"
)

// Server roles
const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

type Server struct {
	Addr       string
	storage    *mcache.Cache[string]
	role       string
	replId     string
	replOffset int
}

func NewServer(addr string) *Server {
	store := mcache.NewCache[string]()

	server := &Server{
		Addr:       addr,
		storage:    store,
		role:       RoleMaster,
		replOffset: 0,
	}

	// Generate a 40-character long replication ID
	// server.replId = fmt.Sprintf("%x", crypto.SHA1.New().Sum([]byte(addr))) // unsopported on codecrafters hardware
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	for range 40 {
		server.replId += string(letters[rand.Intn(len(letters))])
	}

	return server
}

func (s *Server) AsSlaveOf(masterAddr string) {
	s.role = RoleSlave
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		log.Printf("[INFO] New connection from: %v (%v)", conn.RemoteAddr(), conn)
		go s.handleConnection(conn)
	}
}

// Response Data types
const (
	TypeSimpleString = '+'
	TypeSimpleError  = '-'
	TypeInteger      = ':'
	TypeBulkString   = '$'
	TypeArray        = '*'
)

// handleConnection will read data from the connection
func (s *Server) handleConnection(connection net.Conn) error {
	reader := bufio.NewReader(connection)

	for {
		cmd, err := reader.ReadByte()
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("[INFO] Connection closed (EOF), %v", connection)
				connection.Close()
				return nil
			} else {
				err = fmt.Errorf("error reading data: %w", err)
				log.Printf("[ERROR] %s", err)
				return err
			}
		}

		switch cmd {
		case TypeArray:
			a, err := s.readArray(reader)
			if err != nil {
				log.Printf("[ERROR] error reading array: %e", err)
			}
			log.Printf("[DEBUG] Array: %v", a)

			if len(a) == 0 {
				continue
			}
			switch strings.ToUpper(a[0]) {
			case "PING":
				connection.Write([]byte(s.makeSimpleString("PONG")))
			case "ECHO":
				if len(a) != 2 {
					connection.Write([]byte(s.makeSimpleString("ERR wrong number of arguments for 'echo' command")))
					continue
				}
				connection.Write([]byte(s.makeBulkString(a[1])))
			case "SET":
				if len(a) < 3 {
					connection.Write([]byte(s.makeSimpleString("ERR wrong number of arguments for 'set' command")))
					continue
				}

				log.Printf("[DEBUG] SET command: %v", a)
				if len(a) == 5 && strings.ToUpper(a[3]) == "PX" {
					exp, err := strconv.Atoi(a[4])
					if err != nil {
						connection.Write([]byte(s.makeSimpleString("ERR invalid expiration")))
						log.Printf("[ERROR] error parsing expiration: %e", err)
						continue
					}
					// Set with expiration
					log.Printf("[DEBUG] Setting key %s with value %s and expiration %s\n", a[1], a[2], a[4])
					s.storage.Set(a[1], a[2], time.Millisecond*time.Duration(exp))
					connection.Write([]byte(s.makeSimpleString("OK")))

					continue
				}
				// Set without expiration
				log.Printf("[DEBUG] Setting key %s with value %s\n", a[1], a[2])
				s.storage.Set(a[1], a[2], 0)

				connection.Write([]byte(s.makeSimpleString("OK")))
			case "GET":
				if len(a) != 2 {
					connection.Write([]byte(s.makeSimpleString("ERR wrong number of arguments for 'get' command")))
					continue
				}
				value, err := s.storage.Get(a[1])
				if err != nil {
					connection.Write([]byte(s.nullBulkString()))
					continue
				}
				connection.Write([]byte(s.makeBulkString(value)))
			case "INFO":
				info := s.getInfo()
				connection.Write([]byte(s.makeBulkString(strings.Join(info, "\r\n"))))
			default:
				connection.Write([]byte(s.makeSimpleString("ERR unknown command")))
			}
		}
	}
}

func (s Server) getInfo() []string {
	info := []string{}
	info = append(info, "Replication")
	info = append(info, "role:"+s.role)
	if s.role == RoleMaster {
		info = append(info, fmt.Sprintf("master_replid:%s", s.replId))
		info = append(info, fmt.Sprintf("master_repl_offset:%d", s.replOffset))
	}
	return info
}

func (s *Server) readArray(reader *bufio.Reader) ([]string, error) {
	result := []string{}

	// Parse the header to get the number of elements in the array
	arrLen := 0
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	header = strings.Trim(header, "\r\n")

	fmt.Sscanf(header, "%d", &arrLen)
	log.Printf("[DEBUG] array length parsed: %d", arrLen)

	for i := 0; i < arrLen; i++ {
		// Read the header of the element ($<element length>)
		data, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		data = strings.Trim(data, "\r\n")
		elementLen := 0
		fmt.Sscanf(data, string(TypeBulkString)+"%d", &elementLen)

		// Read the element data
		data, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		data = strings.Trim(data, "\r\n")

		if len(data) != elementLen {
			return nil, fmt.Errorf("error reading element: expected %d bytes, got %d", elementLen, len(data))
		}

		result = append(result, data)
	}

	return result, nil
}

func (s *Server) makeSimpleString(data string) string {
	return fmt.Sprintf("%c%s\r\n", TypeSimpleString, data)
}

func (s *Server) makeBulkString(data string) string {
	if data == "" {
		return fmt.Sprintf("%c\r\n\r\n", TypeBulkString)
	}
	return fmt.Sprintf("%c%d\r\n%s\r\n", TypeBulkString, len(data), data)
}

func (s *Server) nullBulkString() string {
	return fmt.Sprintf("%c-1\r\n", TypeBulkString)
}

// func (s *Server) makeArray(arr []string) string {
// 	result := fmt.Sprintf("%c%d\r\n", TypeArray, len(arr))
// 	for _, v := range arr {
// 		result += s.makeBulkString(v)
// 	}
// 	return result
// }
