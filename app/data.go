package main

// Methods specific to data handling: reading, parsing, constructing, writing
// Redis protocol data

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
)

// readArray reads an array from the reader and returns as a slice of strings
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

// makeSimpleError returns a simple error response
func (s *Server) makeSimpleError(data string) string {
	return fmt.Sprintf("%c%s\r\n", TypeSimpleError, data)
}

// makeSimpleString returns a simple string response
func (s *Server) makeSimpleString(data string) string {
	return fmt.Sprintf("%c%s\r\n", TypeSimpleString, data)
}

// readSimpleString reads a simple string from the reader
func (s *Server) readSimpleString(reader *bufio.Reader) (string, error) {
	data, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	data = strings.Trim(data, "\r\n")
	return data, nil
}

// makeBulkString returns a bulk string response
func (s *Server) makeBulkString(data string) string {
	if data == "" {
		return fmt.Sprintf("%c\r\n\r\n", TypeBulkString)
	}
	return fmt.Sprintf("%c%d\r\n%s\r\n", TypeBulkString, len(data), data)
}

// nullBulkString returns a null bulk string response
func (s *Server) nullBulkString() string {
	return fmt.Sprintf("%c-1\r\n", TypeBulkString)
}

// makeArray returns an array response
func (s *Server) makeArray(arr []string) string {
	result := fmt.Sprintf("%c%d\r\n", TypeArray, len(arr))
	for _, v := range arr {
		result += s.makeBulkString(v)
	}
	return result
}

// makeRDBFile returns a RDB file response
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
