package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type Server struct {
	Addr string
}

func NewServer(addr string) *Server {
	return &Server{
		Addr: addr,
	}
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

		go s.handleConnection(conn)
	}
}

func main() {
	s := NewServer("0.0.0.0:6379")
	err := s.ListenAndServe()
	if err != nil {
		fmt.Println("Error starting server: ", err)
		os.Exit(1)
	}
}

// Response Data types
const (
	SimpleString = '+'
	SimpleError  = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

func (s *Server) readArray(header string, reader *bufio.Reader) ([]string, error) {
	result := []string{}
	// Parse the header to get the number of elements in the array
	arrLen := 0
	fmt.Sscanf(header, "*%d", &arrLen)
	log.Println("Array length parsed: ", arrLen)
	// Parse the elements
	// reader := bufio.NewReader(s.conn)

	for i := 0; i < arrLen; i++ {
		// Read the header of the element ($<element length>)
		data, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		data = strings.Trim(data, "\r\n")
		elementLen := 0
		fmt.Sscanf(data, "$%d", &elementLen)

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
	return fmt.Sprintf("+%s\r\n", data)
}

func (s *Server) makeBulkString(data string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
}

func (s *Server) makeArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, v := range arr {
		result += s.makeBulkString(v)
	}
	return result
}

// handleConnection will read data from the connection
func (s *Server) handleConnection(connection net.Conn) error {
	reader := bufio.NewReader(connection)

	stop := 0
	for stop < 20 {
		stop++
		data, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Connection closed")
				connection.Close()
				return nil
			} else {
				err = fmt.Errorf("error reading data: %w", err)
				log.Println(err)
				return err
			}
		}

		switch data[0] {
		case SimpleString:
			fmt.Println("SimpleString: ", data)
		case SimpleError:
			fmt.Println("SimpleError: ", data)
		case Integer:
			fmt.Println("Integer: ", data)
		case BulkString:
			fmt.Println("BulkString: ", data)
		case Array:
			// fmt.Println("Array: ", data)
			a, err := s.readArray(data, reader)
			if err != nil {
				log.Println("Error reading array: ", err)
			}
			log.Println("Array: ", a)

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
			}
		}

		// fmt.Println("Received data: ", data)
	}
	return nil
}
