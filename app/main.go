package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	_ "go/types"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// expect a --dir argument and store in dir variable
var dir = flag.String("dir", "", "Directory where RDB file is stored")
var dbfilename = flag.String("dbfilename", "", "Name of the RDB file")

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

func handleConnection(conn net.Conn, dataMap map[string]string, expiryMap map[string]time.Time) {
	defer conn.Close()
	buffer := make([]byte, 1024)

	configCom := map[string]string{ //config command; store string vals here
		"dir":        *dir,
		"dbfilename": *dbfilename,
	}

	//maybe use a switch?
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

		if strings.ToUpper(commands[0]) == "SET" {
			response := Set(commands, dataMap, expiryMap)
			conn.Write([]byte(response))
		}

		if strings.ToUpper(commands[0]) == "GET" {
			response := Get(commands, dataMap, expiryMap)
			conn.Write([]byte(response))
		}

		if strings.ToUpper(commands[0]) == "CONFIG" && strings.ToUpper(commands[1]) == "GET" && len(commands) == 3 {
			parameter := commands[2]
			value, ok := configCom[parameter]
			if !ok {
				conn.Write([]byte("$-1\r\n"))
				continue
			}
			response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(parameter), parameter, len(value), value)
			conn.Write([]byte(response))
		}
		if strings.ToUpper(commands[0]) == "KEYS" && commands[1] == "*" {
			response := fmt.Sprintf("*%d\r\n", len(dataMap)) //sprintf returns formatted string
			for key := range dataMap {
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
			}
			conn.Write([]byte(response))
		}
	}
}

func Set(commands []string, dataMap map[string]string, expiryMap map[string]time.Time) string {
	//ex set name ben
	if len(commands) != 3 && len(commands) != 5 {
		return "Error, wrong length for set command"
	}
	key := commands[1]
	value := commands[2]
	dataMap[key] = value

	if len(commands) == 5 && strings.ToUpper(commands[3]) == "PX" {
		milliStr := commands[4]
		milliSec, err := strconv.Atoi(milliStr) //covert time to an int
		if err != nil {
			return "ERROR: px value must be an int"
		}
		expiryMap[key] = time.Now().Add(time.Duration(milliSec) * time.Millisecond) //covert time conversion to seconds relative to current time and store in new map
	}
	return "+OK\r\n"
}

func Get(commands []string, dataMap map[string]string, expiryMap map[string]time.Time) string {
	//ex get name
	if len(commands) != 2 {
		return "ERROR: wrong length for get command"
	}
	key := commands[1]

	if expTime, ok := expiryMap[key]; ok && time.Now().After(expTime) {
		delete(dataMap, key) //delete entries in maps
		delete(expiryMap, key)
		return "$-1\r\n"
	}

	value, ok := dataMap[key]
	if !ok {
		return "$-1\r\n" //null bulk string
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value) //format
}

// return keys value from file and key with their expiration time
func readRDB(filePath string) (map[string]string, map[string]time.Time, error) {
	dataMap := make(map[string]string)
	dataMapTime := make(map[string]time.Time)

	//check: do we start with "REDIS" and version 0011? else abort
	data, err := os.ReadFile(filePath)
	if err != nil {
		return dataMap, dataMapTime, nil
	}
	if len(data) < 9 {
		return dataMap, dataMapTime, nil
	}
	fmt.Println("Magic:", string(data[:9])) //debug
	if string(data[:9]) != "REDIS0011" {    //covert to string and check if valid magic number
		return dataMap, dataMapTime, nil
	}
	//skip past metadata section
	index := 9
	for index < len(data) && data[index] == 0xFA {
		index++
		fmt.Printf("found metadata at %d\n", index) //debug
		_, err := readString(data, &index)          //read key
		if err != nil {
			return dataMap, dataMapTime, nil
		}

		_, err = readString(data, &index) //read value
		if err != nil {
			return dataMap, dataMapTime, nil
		}
	}

	if index < len(data) && data[index] == 0xFE {
		index++
		index++
	}
	for index < len(data) && data[index] != 0xFF {
		firstByte := data[index]
		index += 1

		switch firstByte {
		case 0x00:
			key, err := readString(data, &index)
			if err != nil {
				return dataMap, dataMapTime, nil
			}
			value, err := readString(data, &index)
			if err != nil {
				return dataMap, dataMapTime, nil
			}
			dataMap[key] = value
			fmt.Printf("Loaded key: '%s', value: '%s'\n", key, value)

		case 0xFD:
			if index+4 > len(data) {
				return dataMap, dataMapTime, fmt.Errorf("expected more bytes for expiration")
			}
			expirationSeconds := binary.LittleEndian.Uint32(data[index : index+4])
			index += 4
			if data[index] != 0x00 {
				return dataMap, dataMapTime, fmt.Errorf("Expected string after expiration")
			}
			index++
			key, err := readString(data, &index)
			if err != nil {
				return dataMap, dataMapTime, nil
			}
			value, err := readString(data, &index)
			if err != nil {
				return dataMap, dataMapTime, nil
			}

			dataMap[key] = value
			fmt.Printf("Loaded key: '%s', value: '%s'\n", key, value)

			dataMapTime[key] = time.Unix(int64(expirationSeconds), 0)

		case 0xFB: //FB starts with size of the hash table that stores the keys and values
			if index+4 > len(data) { //idk
				return dataMap, dataMapTime, fmt.Errorf("undexpected end after after 0xFB")
			}
			_ = binary.LittleEndian.Uint32(data[index : index+4])
			index += 4
			key, err := readString(data, &index)
			if err != nil {
				return dataMap, dataMapTime, nil
			}
			value, err := readString(data, &index)
			if err != nil {
				return dataMap, dataMapTime, nil
			}
			dataMap[key] = value
			fmt.Printf("Loaded key: '%s', value: '%s'\n", key, value)

		default:
			return dataMap, dataMapTime, fmt.Errorf("unsupported entry type: 0x%X at index %d", firstByte, index-1)
		}
	}
	//debugging
	fmt.Printf("readRDB parsed %d keys: \n:", len(dataMap))
	for k, v := range dataMap {
		fmt.Printf("→ %s = %s\n", k, v)
	}
	return dataMap, dataMapTime, nil
}

func readString(data []byte, index *int) (string, error) {
	firstByte := data[*index]
	*index += 1

	prefix := firstByte & 0xC0 //mask the first two bits
	var length int
	var str string

	switch prefix {

	case 0x00: //6 bit
		length = int(firstByte & 0x3F)

	case 0x40: //14 bit
		if *index >= len(data) {
			return "", fmt.Errorf("Error end of data(14 bit)")
		}
		secondByte := data[*index]
		*index += 1
		length = int(firstByte&0x3f)<<8 | int(secondByte)

	case 0x80: // 32 bit
		if *index+4 > len(data) {
			return "", fmt.Errorf("not enough bytes")
		}
		length = int(binary.BigEndian.Uint32(data[*index : *index+4]))
		*index += 4

	case 0xC0: //special case
		return "", fmt.Errorf("unsupported string encoding")
	}

	if *index+length > len(data) {
		return "", fmt.Errorf("not enough data to read string of %d", length)
	}
	str = string(data[*index : *index+length])
	*index += length
	return str, nil
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	flag.Parse() //parse input for command vals(RDB)

	filePath := path.Join(*dir, *dbfilename)

	dataMapKeys, expiryMapKeys, err := readRDB(filePath)
	if err != nil {
		fmt.Println("failed to load RDB file")
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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
		go handleConnection(conn, dataMapKeys, expiryMapKeys) //handles each connection in its own goroutine
	}
}
