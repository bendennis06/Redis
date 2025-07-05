package main

import (
	"fmt"
	_ "go/types"
	"net"
	"os"
	"strconv"
	"strings"
)

// Take the raw bytes (or string) your server reads from the connection.
// Parse the Redis RESP protocol.
// Return a slice []string → the command name and its arguments.
// //ex *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
func parseRESP(b []byte) ([]string, error) {

	if len(b) == 0 {
		return nil, fmt.Errorf("input to short") //no data to read
	}

	if b[0] != '*' {
		return nil, fmt.Errorf("expected array at top level") //needs to start with '*'
	}

	index := 1
	//look for '\r\n'
	for index < len(b) && !(b[index] == '\r' && b[index+1] == '\n') {
		index++
	}

	arraySizeString := string(b[1:index])            //string slice, grab array length
	arrSizeInt, err := strconv.Atoi(arraySizeString) //convert string to int
	if err != nil {
		return nil, fmt.Errorf("not valid array size", err)
	}

	index += 2 //skip past '\r\n

	parts := []string{}               //[] slice(dynamic array) that holds strings, {} ini as empty
	for j := 0; j < arrSizeInt; j++ { //runs for each num of strings (*n) will run n times
		if b[index] != '$' {
			return nil, fmt.Errorf("expected bulk string", b[index]) //look for '$' (bulk strings)
		}
		index++

		startInt := index //start of string
		for index < len(b) && !(b[index] == '\r' && b[index+1] == '\n') {
			index++
		}
		bulkLenStr := string(b[startInt:index])  //length of string (num after $)
		bulkLen, err := strconv.Atoi(bulkLenStr) //convert length to int
		if err != nil {
			return nil, fmt.Errorf("bad bulk length", err)
		}
		index += 2                               //skip past '\r\n'
		part := string(b[index : index+bulkLen]) //extract bulk string
		parts = append(parts, part)

		index += bulkLen + 2 //move past string and last \r\n
	}
	return parts, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}

		//parse resp
		commands, err := parseRESP(buffer[:n])
		if err != nil {
			fmt.Println("error parsing", err)
			continue
		}

		//check command is ping
		if len(commands) == 1 && strings.ToUpper(commands[0]) == "PING" {
			conn.Write([]byte("+PONG\r\n"))
		}
		//check if command is echo
		if len(commands) == 2 && strings.ToUpper(commands[0]) == "ECHO" {
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(commands[1]), commands[1]) //build a formatted string
			conn.Write([]byte(response))                                            // return formatted string
		}
		//if input == "*1\r\n$4\r\nPING\r\n" {
		//	conn.Write([]byte("+PONG\r\n"))
		//}
	}
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept() //go's short variable declaration := declares and assigns at the same time
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
			//os.Exit(1)
		}
		go handleConnection(conn) //handles each connection in its own goroutine
	}
}
