package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"net"
	"slices"
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
	Addr         string
	storage      *mcache.Cache[string]
	role         string
	replId       string
	replOffset   int
	capabilities []string
	masterConn   net.Conn
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
	// server.replId = fmt.Sprintf("%x", crypto.SHA1.New().Sum([]byte(addr)))
	// unsopported on codecrafters hardware
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	for range 40 {
		server.replId += string(letters[rand.Intn(len(letters))])
	}

	return server
}

func (s *Server) AsSlaveOf(masterAddr string) error {
	s.role = RoleSlave

	// Connect to the master
	var err error
	s.masterConn, err = net.Dial("tcp", masterAddr)
	if err != nil {
		log.Fatalf("[ERROR] error connecting to master: %e", err)
	}

	// Send PING command
	s.masterConn.Write([]byte(s.makeArray([]string{"PING"})))
	typeResponse, args, err := s.readInput(s.masterConn)
	if err != nil {
		log.Printf("[ERROR] error reading response from master: %e", err)
		return err
	}
	if typeResponse != TypeSimpleString || args[0] != "PONG" {
		err = fmt.Errorf("error connecting to master: invalid response (%v)", args)
		log.Printf("[ERROR] %e", err)
		return err
	}
	log.Printf("[DEBUG] Received PONG from master (%s)", masterAddr)

	// Send REPLCONF listening-port <PORT>
	_, port, err := net.SplitHostPort(s.Addr)
	if err != nil {
		log.Printf("[ERROR] error parsing server address: %e", err)
		return err
	}
	s.masterConn.Write([]byte(s.makeArray([]string{"REPLCONF", "listening-port", port})))
	typeResponse, args, err = s.readInput(s.masterConn)
	if err != nil {
		log.Printf("[ERROR] error reading response from master: %e", err)
		return err
	}
	if typeResponse != TypeSimpleString || args[0] != "OK" {
		err = fmt.Errorf("error connecting to master: invalid response (%v)", args)
		log.Printf("[ERROR] %e", err)
		return err
	}

	// Send REPLCONF capa psync2
	s.masterConn.Write([]byte(s.makeArray([]string{"REPLCONF", "capa", "psync2"})))
	typeResponse, args, err = s.readInput(s.masterConn)
	if err != nil {
		log.Printf("[ERROR] error reading response from master: %e", err)
		return err
	}
	if typeResponse != TypeSimpleString || args[0] != "OK" {
		err = fmt.Errorf("error connecting to master: invalid response (%v)", args)
		log.Printf("[ERROR] %e", err)
		return err
	}

	// Send PSYNC ? -1 to ask for a full synchronization
	s.masterConn.Write([]byte(s.makeArray([]string{"PSYNC", "?", "-1"})))
	typeResponse, args, err = s.readInput(s.masterConn)
	if err != nil {
		log.Printf("[ERROR] error reading response from master: %e", err)
		return err
	}
	if len(args) != 1 {
		err = fmt.Errorf("error connecting to master: invalid response (%v)", args)
		log.Printf("[ERROR] %e", err)
		return err
	}
	args = strings.Split(args[0], " ") // FULLRESYNC <replid> <offset>
	if typeResponse != TypeSimpleString || args[0] != "FULLRESYNC" {
		err = fmt.Errorf("error connecting to master: invalid response (%v)", args)
		log.Printf("[ERROR] %e", err)
		return err
	}

	return nil
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
	for {
		// Read the input
		typeResponse, args, err := s.readInput(connection)
		if err != nil {
			if err.Error() == "EOF" {
				connection.Close()
				return nil
			}
			return err
		}

		// Check the type of response
		if typeResponse != TypeArray {
			connection.Write([]byte(s.makeSimpleString("ERR invalid command")))
			continue
		}

		// Handle the command
		err = s.handleCommand(args, connection)
		if err != nil {
			log.Printf("[ERROR] error handling command: %e", err)
		}

	}
}

func (s *Server) readInput(connection net.Conn) (typeResponse rune, args []string, err error) {
	reader := bufio.NewReader(connection)
	cmd, err := reader.ReadByte()
	if err != nil {
		if err.Error() == "EOF" {
			log.Printf("[INFO] Connection closed (EOF), %v", connection)
			return TypeSimpleError, nil, err
		} else {
			err = fmt.Errorf("error reading data: %w", err)
			log.Printf("[DEBUG] %s", err)
			return TypeSimpleError, nil, err
		}
	}

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
	}

	return TypeSimpleError, nil, fmt.Errorf("invalid command: not an array")
}

func (s *Server) handleCommand(args []string, connection net.Conn) error {
	var err error

	switch strings.ToUpper(args[0]) {
	case "PING":
		log.Printf("[DEBUG] PING command: %v", args)
		connection.Write([]byte(s.makeSimpleString("PONG")))

	case "ECHO":
		log.Printf("[DEBUG] ECHO command: %v", args)
		if len(args) < 2 {
			err = fmt.Errorf("wrong number of arguments for 'echo' command")
			connection.Write([]byte(s.makeSimpleError(err.Error())))
			return err
		}
		connection.Write([]byte(s.makeBulkString(args[1])))

	case "SET":
		log.Printf("[DEBUG] SET command: %v", args)

		if len(args) < 3 {
			err = fmt.Errorf("wrong number of arguments for 'set' command")
			connection.Write([]byte(s.makeSimpleError(err.Error())))
			return err
		}

		if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
			exp, err := strconv.Atoi(args[4])
			if err != nil {
				err = fmt.Errorf("error parsing expiration: %w", err)
				connection.Write([]byte(s.makeSimpleError(err.Error())))
				log.Printf("[ERROR] %e", err)
				return err
			}
			// Set with expiration
			log.Printf("[DEBUG] Setting key %s with value %s and expiration %s\n",
				args[1], args[2], args[4])
			s.storage.Set(args[1], args[2], time.Millisecond*time.Duration(exp))
			connection.Write([]byte(s.makeSimpleString("OK")))

			return nil
		}
		// Set without expiration
		log.Printf("[DEBUG] Setting key %s with value %s\n", args[1], args[2])
		s.storage.Set(args[1], args[2], 0)
		connection.Write([]byte(s.makeSimpleString("OK")))

	case "GET":
		log.Printf("[DEBUG] GET command: %v", args)
		if len(args) != 2 {
			err = fmt.Errorf("wrong number of arguments for 'get' command")
			connection.Write([]byte(s.makeSimpleError(err.Error())))
			return err
		}
		value, err := s.storage.Get(args[1])
		if err != nil {
			connection.Write([]byte(s.nullBulkString()))
			return nil
		}
		connection.Write([]byte(s.makeBulkString(value)))

	case "INFO":
		info := s.getInfo()
		connection.Write([]byte(s.makeBulkString(strings.Join(info, "\r\n"))))
		log.Printf("[DEBUG] INFO command: %v", info)

	case "REPLCONF":
		err = s.replConf(args)
		if err != nil {
			connection.Write([]byte(s.makeSimpleError(err.Error())))
			return err
		}
		connection.Write([]byte(s.makeSimpleString("OK")))

	case "PSYNC":
		err = s.psyncConfig(args)
		if err != nil {
			connection.Write([]byte(s.makeSimpleError(err.Error())))
			return err
		}
		connection.Write([]byte(s.makeSimpleString(fmt.Sprintf("FULLRESYNC %s %d", s.replId, s.replOffset))))

		// Send RDB data
		rdbLen, rdbData, err := s.makeRDBFile()
		if err != nil {
			log.Printf("[ERROR] error generating RDB data: %e", err)
			connection.Write([]byte(s.makeSimpleError(err.Error())))
			return err
		}
		connection.Write([]byte(fmt.Sprintf("%c%d\r\n", TypeBulkString, rdbLen)))
		connection.Write(rdbData)

	default:
		connection.Write([]byte(s.makeSimpleString("ERR unknown command")))
	}
	return nil
}

func (s *Server) psyncConfig(args []string) error {
	if len(args) < 3 {
		err := fmt.Errorf("wrong number of arguments for 'psync' command")
		return err
	}
	// Further replication configuration ...
	if !slices.Contains(s.capabilities, "psync2") {
		err := fmt.Errorf("unsupported PSYNC capabilities")
		return err
	}

	return nil
}

func (s *Server) replConf(args []string) error {
	if len(args) < 3 {
		err := fmt.Errorf("wrong number of arguments for 'replconf' command")
		return err
	}
	// Further replication configuration ...
	for i := 1; i < len(args); i += 2 {
		switch strings.ToLower(args[i]) {
		case "listening-port":
			// Listening port configuration
		case "capa":
			// Capabilities configuration
			s.capabilities = append(s.capabilities, args[i+1])
		}
	}

	return nil
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
			return nil, fmt.Errorf("error reading element: expected %d bytes, got %d",
				elementLen, len(data))
		}

		result = append(result, data)
	}

	return result, nil
}

func (s *Server) makeSimpleError(data string) string {
	return fmt.Sprintf("%c%s\r\n", TypeSimpleError, data)
}

func (s *Server) makeSimpleString(data string) string {
	return fmt.Sprintf("%c%s\r\n", TypeSimpleString, data)
}

func (s *Server) readSimpleString(reader *bufio.Reader) (string, error) {
	data, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	data = strings.Trim(data, "\r\n")
	return data, nil
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

func (s *Server) makeArray(arr []string) string {
	result := fmt.Sprintf("%c%d\r\n", TypeArray, len(arr))
	for _, v := range arr {
		result += s.makeBulkString(v)
	}
	return result
}

func (s *Server) makeRDBFile() (int, []byte, error) {
	// hardcode file content for now
	base64Content := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	// decode into binary
	decoded, err := base64.StdEncoding.DecodeString(base64Content)
	if err != nil {
		return 0, []byte{}, err
	}

	return len(decoded), decoded, nil
}
