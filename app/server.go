package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(connection)
	}

}

// handleConnection will read data from the connection
func handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

	stop := 0
	for stop < 20 {
		stop++
		data, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			return
		}

		fmt.Println("Received data: ", data)

		if strings.Contains(data, "ECHO") {
			conn.Write([]byte(fmt.Sprintf("Received data: %s\r\n", data)))
		}

		if strings.Contains(data, "PING") {
			conn.Write([]byte("+PONG\r\n"))
		}

	}

}
