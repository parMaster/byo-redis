package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parMaster/mcache"
)

// Server roles
const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

type Replica struct {
	Addr string
	Port int
	// replId       string
	// replOffset   int
	conn         net.Conn
	capabilities []string
}

type Server struct {
	Addr         string
	storage      *mcache.Cache[string]
	role         string
	replId       string
	replOffset   int
	replicas     map[string]Replica
	capabilities []string
	masterConn   net.Conn
	mx           sync.Mutex
}

func NewServer(addr string) *Server {
	store := mcache.NewCache[string]()

	server := &Server{
		Addr:         addr,
		storage:      store,
		role:         RoleMaster,
		replOffset:   0,
		capabilities: []string{"psync2", "eof"},
		replicas:     make(map[string]Replica),
		mx:           sync.Mutex{},
	}

	// Generate a 40-character long replication ID
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	for range 40 {
		server.replId += string(letters[rand.Intn(len(letters))])
	}

	return server
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
		go s.handleConnection(conn, false)
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
func (s *Server) handleConnection(connection net.Conn, silent bool) error {
	reader := bufio.NewReader(connection)
	for {
		// Read the input
		typeResponse, args, err := s.readInput(reader)
		log.Printf("[DEBUG] [%s] handleConnection input parsed, %c:%v:%e, &%v",
			s.role, typeResponse, args, err, connection)

		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("[DEBUG] (EOF) reached, %v", connection)
				connection.Close()
				return nil
			}
			log.Printf("[DEBUG] [%s] handleConnection error reading input, %v", s.role, connection)
			return err
		}

		// Check the type of response
		switch typeResponse {
		case TypeArray:
			// Handle the command
			err = s.handleCommand(args, connection)
			if err != nil {
				log.Printf("[ERROR] error handling command: %e", err)
			}
			continue
		case TypeSimpleError:
			log.Printf("[DEBUG] [%s] simple error received: %v", s.role, args)
			continue
		case TypeSimpleString:
			log.Printf("[DEBUG] [%s] simple string received: %v", s.role, args)
			continue
		default:
			log.Printf("[DEBUG] [%s], invalid command: %v", s.role, args)
			if !silent {
				connection.Write([]byte(s.RESPSimpleError("invalid command")))
			}
			continue
		}

	}
}

func (s *Server) readInput(reader *bufio.Reader) (typeResponse rune, args []string, err error) {
	cmd, err := reader.ReadByte()
	if err != nil {
		if err.Error() == "EOF" {
			log.Printf("[INFO] Connection closed (EOF)")
			return TypeSimpleError, nil, err
		} else {
			err = fmt.Errorf("error reading data: %w", err)
			log.Printf("[DEBUG] %s", err)
			return TypeSimpleError, nil, err
		}
	}

	// log.Printf("[DEBUG] readInput cmd: %c", cmd)
	switch cmd {
	case TypeArray:
		args, err := s.readArray(reader)
		if err != nil {
			log.Printf("[ERROR] error reading array: %e", err)
		}
		log.Printf("[DEBUG] Array: %v", args)

		if len(args) == 0 {
			return TypeSimpleError, nil, fmt.Errorf("empty array")
		}

		return TypeArray, args, nil

	case TypeSimpleString:
		data, err := s.readSimpleString(reader)
		if err != nil {
			log.Printf("[ERROR] error reading simple string: %e", err)
		}
		log.Printf("[DEBUG] Simple string: %s", data)

		return TypeSimpleString, []string{data}, nil
	case TypeSimpleError:
		data, err := s.readSimpleError(reader)
		if err != nil {
			log.Printf("[ERROR] error reading simple error: %e", err)
		}
		log.Printf("[DEBUG] Simple error: %s", data)
		return TypeSimpleError, []string{data}, nil
	}

	return TypeSimpleError, nil, fmt.Errorf("\"%c\" type not supported", cmd)
}

func (s *Server) handleCommand(args []string, connection net.Conn) error {
	var err error

	switch strings.ToUpper(args[0]) {
	case "PING":
		log.Printf("[DEBUG] [%s] PING command: %v", s.role, args)
		connection.Write([]byte(s.RESPSimpleString("PONG")))

	case "ECHO":
		log.Printf("[DEBUG] [%s] ECHO command: %v", s.role, args)
		if len(args) < 2 {
			err = fmt.Errorf("wrong number of arguments for 'echo' command")
			connection.Write([]byte(s.RESPSimpleError(err.Error())))
			return err
		}
		connection.Write([]byte(s.RESPBulkString(args[1])))

	case "SET":
		log.Printf("[DEBUG] [%s] SET command: %v", s.role, args)

		if len(args) < 3 {
			err = fmt.Errorf("wrong number of arguments for 'set' command")
			connection.Write([]byte(s.RESPSimpleError(err.Error())))
			return err
		}

		if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
			exp, err := strconv.Atoi(args[4])
			if err != nil {
				err = fmt.Errorf("error parsing expiration: %w", err)
				connection.Write([]byte(s.RESPSimpleError(err.Error())))
				log.Printf("[ERROR] %e", err)
				return err
			}
			// Set with expiration
			log.Printf("[DEBUG] [%s] Setting key %s with value %s and expiration %s\n",
				s.role, args[1], args[2], args[4])
			s.storage.Set(args[1], args[2], time.Millisecond*time.Duration(exp))
			connection.Write([]byte(s.RESPSimpleString("OK")))
			s.propagate(args)

			return nil
		}
		// Set without expiration
		log.Printf("[DEBUG] [%s] Setting key %s with value %s\n", s.role, args[1], args[2])

		s.storage.Set(args[1], args[2], 0)
		connection.Write([]byte(s.RESPSimpleString("OK")))
		s.propagate(args)

	case "GET":
		log.Printf("[DEBUG] [%s] GET command: %v", s.role, args)
		if len(args) != 2 {
			err = fmt.Errorf("wrong number of arguments for 'get' command")
			connection.Write([]byte(s.RESPSimpleError(err.Error())))
			return err
		}
		value, err := s.storage.Get(args[1])
		if err != nil {
			connection.Write([]byte(s.nullBulkString()))
			return nil
		}
		connection.Write([]byte(s.RESPBulkString(value)))

	case "INFO":
		info := s.getInfo()
		connection.Write([]byte(s.RESPBulkString(strings.Join(info, "\r\n"))))
		log.Printf("[DEBUG] INFO command: %v", info)

	case "REPLCONF":
		if s.role != RoleMaster {
			err = fmt.Errorf("REPLCONF command is only valid for master servers")
			connection.Write([]byte(s.RESPSimpleError(err.Error())))
			return err
		}

		s.mx.Lock()
		// replAddr is a temp session ID, since handshake is a single connection
		replAddr := connection.RemoteAddr().String()
		err = s.replConf(replAddr, args)
		if err != nil {
			connection.Write([]byte(s.RESPSimpleError(err.Error())))
			return err
		}

		if repl, ok := s.replicas[replAddr]; ok {
			repl.conn = connection
			s.replicas[replAddr] = repl
		}
		s.mx.Unlock()

		connection.Write([]byte(s.RESPSimpleString("OK")))

	case "PSYNC":
		err = s.psyncConfig(args)
		if err != nil {
			connection.Write([]byte(s.RESPSimpleError(err.Error())))
			return err
		}
		connection.Write([]byte(s.RESPSimpleString(fmt.Sprintf("FULLRESYNC %s %d", s.replId, s.replOffset))))

		// handshake is complete, replace temp session ID with the actual replica address
		s.mx.Lock()
		replAddr := connection.RemoteAddr().String()
		if repl, ok := s.replicas[replAddr]; ok {
			s.replicas[net.JoinHostPort(repl.Addr, strconv.Itoa(repl.Port))] = repl
			delete(s.replicas, replAddr)
		}
		s.mx.Unlock()

		// start the replication
		// Send RDB data
		rdbLen, rdbData, err := s.makeRDBFile()
		if err != nil {
			log.Printf("[ERROR] error generating RDB data: %e", err)
			connection.Write([]byte(s.RESPSimpleError(err.Error())))
			return err
		}
		connection.Write([]byte(fmt.Sprintf("%c%d\r\n", TypeBulkString, rdbLen)))
		connection.Write(rdbData)

	default:
		connection.Write([]byte(s.RESPSimpleString("ERR unknown command")))
	}
	return nil
}

func (s *Server) getInfo() []string {
	info := []string{}
	info = append(info, "Replication")
	info = append(info, "role:"+s.role)
	if s.role == RoleMaster {
		info = append(info, fmt.Sprintf("master_replid:%s", s.replId))
		info = append(info, fmt.Sprintf("master_repl_offset:%d", s.replOffset))
	}
	return info
}
