package main

// Methods specific to replication handling

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// AsSlaveOf sets the server as a slave of the given master
// shaking hands with the master and asking for a full synchronization
func (s *Server) AsSlaveOf(masterAddr string) error {
	s.role = RoleSlave

	// Connect to the master
	var err error
	s.masterConn, err = net.Dial("tcp", masterAddr)
	if err != nil {
		log.Fatalf("[ERROR] error connecting to master: %e", err)
	}

	reader := bufio.NewReader(s.masterConn)

	// Send PING command
	s.masterConn.Write([]byte(s.RESPArray([]string{"PING"})))
	typeResponse, args, err := s.readInput(reader)
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
	s.masterConn.Write([]byte(s.RESPArray([]string{"REPLCONF", "listening-port", port})))
	typeResponse, args, err = s.readInput(reader)
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
	s.masterConn.Write([]byte(s.RESPArray([]string{"REPLCONF", "capa", "psync2"})))
	typeResponse, args, err = s.readInput(reader)
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
	s.masterConn.Write([]byte(s.RESPArray([]string{"PSYNC", "?", "-1"})))
	typeResponse, args, err = s.readInput(reader)
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
	log.Printf("[DEBUG] Received FULLRESYNC from master (%s): %v", masterAddr, args)

	// Start the synchronization process
	// read out $<length>\r\n<bulk data>
	// there's no \r\n at the end of the bulk data
	reader.ReadByte() // $
	strLength, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[ERROR] error reading length of bulk data: %e", err)
		return err
	}
	strLength = strings.Trim(strLength, "\r\n")
	length, err := strconv.Atoi(strLength)
	if err != nil {
		log.Printf("[ERROR] error parsing length of bulk data: %e", err)
		return err
	}
	log.Printf("[DEBUG] length of bulk data: %d", length)
	buf := make([]byte, length)
	n, err := reader.Read(buf)
	if err != nil {
		log.Printf("[ERROR] error reading bulk data: %e", err)
		return err
	}
	log.Printf("[DEBUG] %d bytes read from master", n)

	go s.handleReplication(s.masterConn, reader)

	return nil
}

// handleReplication reads the input from the master and handles the replication
// reusing the same connection and reader
func (s *Server) handleReplication(connection net.Conn, reader *bufio.Reader) error {
	for {
		// Read the input
		typeResponse, args, err := s.readInput(reader)
		log.Printf("[DEBUG] [repl] handleReplication input parsed, %c:%v:%e, &%v",
			typeResponse, args, err, connection)

		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("[DEBUG] (EOF) reached, %v", connection)
				connection.Close()
				return nil
			}
			log.Printf("[DEBUG] [repl] handleReplication error reading input, %v", connection)
			return err
		}

		switch typeResponse {
		case TypeArray:
			err = s.handleReplCommand(args, connection)
			if err != nil {
				log.Printf("[ERROR] [repl] error handling command: %e", err)
			}
			continue
		case TypeSimpleError:
			log.Printf("[DEBUG] [repl] simple error received: %v", args)
			continue
		case TypeSimpleString:
			log.Printf("[DEBUG] [repl] simple string received: %v", args)
			continue
		default:
			log.Printf("[DEBUG] [repl], invalid command: %v", args)
			continue
		}

	}
}

// handleReplCommand handles the replication commands over the connection to the master.
// SET happens silently without propagation, no response is sent back to the master.
// Implements REPLCONF GETACK * and so on
func (s *Server) handleReplCommand(args []string, connection net.Conn) error {
	var err error

	switch strings.ToUpper(args[0]) {

	case "SET":
		log.Printf("[DEBUG] [%s] SET command: %v", s.role, args)

		if len(args) < 3 {
			err = fmt.Errorf("wrong number of arguments for 'set' command")
			return err
		}

		if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
			exp, err := strconv.Atoi(args[4])
			if err != nil {
				err = fmt.Errorf("error parsing expiration: %w", err)
				log.Printf("[ERROR] %e", err)
				return err
			}
			// Set with expiration
			log.Printf("[DEBUG] [%s] Setting key %s with value %s and expiration %s\n",
				s.role, args[1], args[2], args[4])
			s.storage.Set(args[1], args[2], time.Millisecond*time.Duration(exp))
			return nil
		}
		// Set without expiration
		log.Printf("[DEBUG] [%s] Setting key %s with value %s\n", s.role, args[1], args[2])

		s.storage.Set(args[1], args[2], 0)

		s.replOffset += len(s.RESPArray(args))
		log.Printf("[DEBUG] [%s] replOffset: %d", s.role, s.replOffset)

	case "REPLCONF":

		log.Printf("[DEBUG] [%s] REPLCONF command: %v", s.role, args)

		if len(args) < 3 {
			err = fmt.Errorf("wrong number of arguments for 'replconf' command")
			return err
		}

		// REPLCONF GETACK *
		if strings.ToUpper(args[1]) == "GETACK" && args[2] == "*" {
			// REPLCONF ACK <offset>
			connection.Write([]byte(s.RESPArray([]string{"REPLCONF", "ACK", strconv.Itoa(s.replOffset)})))
		}

		s.replOffset += len(s.RESPArray(args))
		log.Printf("[DEBUG] [%s] replOffset: %d", s.role, s.replOffset)

	default:
		connection.Write([]byte(s.RESPSimpleError("unknown command")))
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

// Server configuration for replication
func (s *Server) replConf(replAddr string, args []string) error {
	if len(args) < 3 || len(args)%2 != 1 {
		err := fmt.Errorf("wrong number of arguments for 'replconf' command")
		return err
	}

	repl, ok := s.replicas[replAddr]
	if !ok {
		ra, _, _ := net.SplitHostPort(replAddr)
		repl = Replica{
			Addr:         ra,
			capabilities: []string{},
		}
	}

	// Further replication configuration ...
	for i := 1; i < len(args); i += 2 {
		switch strings.ToLower(args[i]) {
		case "listening-port":
			// Listening port configuration
			port, err := strconv.Atoi(args[i+1])
			if err != nil {
				return err
			}
			repl.Port = port
		case "capa":
			// Capabilities configuration
			repl.capabilities = append(repl.capabilities, args[i+1])
		}
		s.replicas[replAddr] = repl
	}

	return nil
}

// primitive function to propagate a command to all replicas
func (s *Server) propagate(args []string) error {

	for ra, repl := range s.replicas {
		log.Printf("[DEBUG] -> Propagating to %s, args: %v", ra, args)
		n, err := repl.conn.Write([]byte(s.RESPArray(args)))
		if err != nil {
			log.Printf("[ERROR] error writing to replica %s: %e, trying to reconnect", ra, err)
			// try to reconnect
			conn, err := net.Dial("tcp", net.JoinHostPort(repl.Addr, strconv.Itoa(repl.Port)))
			if err != nil {
				log.Printf("[ERROR] error reconnecting to replica %s: %e", ra, err)
				continue
			}
			repl.conn = conn
			s.replicas[ra] = repl
			n, err = repl.conn.Write([]byte(s.RESPArray(args)))
			if err != nil {
				log.Printf("[ERROR] error writing to reconnected replica %s: %e", ra, err)
				continue
			}
		}
		log.Printf("[DEBUG] %d bytes written to replica %s", n, ra)
	}

	return nil
}
