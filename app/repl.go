package main

// Methods specific to replication handling

import (
	"fmt"
	"log"
	"net"
	"slices"
	"strconv"
	"strings"
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
	log.Printf("[DEBUG] Received FULLRESYNC from master (%s): %v", masterAddr, args)

	go s.handleConnection(s.masterConn, true)

	// Start the synchronization process
	// read out $<length>\r\n<bulk data>
	// there's no \r\n at the end of the bulk data
	// reader := bufio.NewReader(s.masterConn)
	// reader.ReadByte() // $
	// strLength, err := reader.ReadString('\n')
	// if err != nil {
	// 	log.Printf("[ERROR] error reading length of bulk data: %e", err)
	// 	return err
	// }
	// strLength = strings.Trim(strLength, "\r\n")
	// length, err := strconv.Atoi(strLength)
	// if err != nil {
	// 	log.Printf("[ERROR] error parsing length of bulk data: %e", err)
	// 	return err
	// }
	// log.Printf("[DEBUG] length of bulk data: %d", length)
	// buf := make([]byte, length)
	// n, err := reader.Read(buf)
	// if err != nil {
	// 	log.Printf("[ERROR] error reading bulk data: %e", err)
	// 	return err
	// }
	// log.Printf("[DEBUG] %d bytes read from master", n)

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
		log.Printf("[DEBUG] Propagating to %s, args: %v", ra, args)
		n, err := repl.conn.Write([]byte(s.makeArray(args)))
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
			n, err = repl.conn.Write([]byte(s.makeArray(args)))
			if err != nil {
				log.Printf("[ERROR] error writing to reconnected replica %s: %e", ra, err)
				continue
			}
		}
		log.Printf("[DEBUG] %d bytes written to replica %s", n, ra)
	}

	return nil
}
