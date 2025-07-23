package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	_ "go/types"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type StoreValue struct {
	value  string
	expiry *time.Time
}

var Store = make(map[string]StoreValue)

var cache map[string]bool

var role = "master"

func handleConnection(conn net.Conn, dir string, dbfilename string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := parseRESP(reader)
		if err != nil {
			fmt.Println("error reading from connection ", err)
		}
		if len(line) == 0 { //test
			return
		}
		//parse resp
		command := strings.ToUpper(line[0])
		switch command {
		case "ECHO":
			conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(line[1]), line[1]))
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "SET":
			if len(line) > 3 && strings.ToUpper(line[3]) == "PX" {
				px, err := strconv.Atoi(line[4])
				if err != nil {
					fmt.Println("expiry should be int")
					continue
				}
				expiry := time.Now().Add(time.Millisecond * time.Duration(px))
				Store[line[1]] = StoreValue{line[2], &expiry}
			} else {
				Store[line[1]] = StoreValue{line[2], nil}
			}
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			val, ok := Store[line[1]]
			if !ok || (val.expiry != nil) && time.Now().After(*val.expiry) {
				conn.Write([]byte("$-1\r\n"))
			} else {
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val.value), val.value))
			}
		case "CONFIG":
			if line[2] == "dir" {
				conn.Write(fmt.Appendf(nil, "*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(dir), dir))
			} else if line[2] == "dbfilename" {
				conn.Write(fmt.Appendf(nil, "*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(dbfilename), dbfilename))
			}
		case "KEYS":
			conn.Write([]byte(outputRESP(FindKeys(line[1]))))
		case "INFO":
			handleInfo(conn, line[1:])
		case "REPLCONF":
			conn.Write([]byte("+OK\r\n"))
		case "PSYNC":
			conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
			RDBcontent, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
			conn.Write([]byte(fmt.Sprintf("$%v\r\n%v", len(string(RDBcontent)), string(RDBcontent))))
		}
	}
}

func handleInfo(conn net.Conn, args []string) {
	if len(args) > 0 && strings.ToUpper(args[0]) == "REPLICATION" {
		info := []string{
			fmt.Sprintf("role:%s", role),
			"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			"master_repl_offset:0",
		}
		joined := strings.Join(info, "\r\n")
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(joined), joined)

		conn.Write([]byte(resp))
	}
}

func FindKeys(pattern string) []string {
	output := []string{}
	for key := range Store {
		cache = make(map[string]bool)
		if matchString(pattern, key, 0, 0) {
			output = append(output, key)
		}
	}
	return output
}

func matchString(pattern, word string, i, j int) bool {
	if i >= len(pattern) {
		return j >= len(word)
	}

	if j >= len(word) {
		for i < len(pattern) && pattern[i] == '*' {
			i++
		}
		return i == len(pattern)
	}
	key := strconv.Itoa(i) + ", " + strconv.Itoa(j)
	val, ok := cache[key]
	if ok {
		return val
	}
	if pattern[i] == word[j] {
		cache[key] = matchString(pattern, word, i+1, j+1)
	} else if pattern[i] == '*' {
		cache[key] = matchString(pattern, word, i+1, j) || matchString(pattern, word, i, j+1)
	} else {
		cache[key] = false
	}
	return cache[key]
}

func parseRESP(r *bufio.Reader) ([]string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, err
	}

	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		prefix, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if prefix != '$' {
			return nil, fmt.Errorf("expected $")
		}
		lengthLine, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lengthLine = strings.TrimSpace(lengthLine)
		length, err := strconv.Atoi(lengthLine)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length ", err)
		}
		buf := make([]byte, length)
		_, err = r.Read(buf)
		if err != nil {
			return nil, err
		}
		if _, err := r.ReadByte(); err != nil {
			return nil, err
		}
		if _, err := r.ReadByte(); err != nil {
			return nil, err
		}
		result = append(result, string(buf))
	}
	return result, nil
}

func outputRESP(items []string) string {
	var b strings.Builder
	// Write the array header: *<num items>
	b.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
	for _, item := range items {
		// Each item is a bulk string: $<length>\r\n<item>\r\n
		b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
	}
	return b.String()
}

func readString(b *bytes.Reader) (string, error) {
	length, err, asInt := readLength(b)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	binary.Read(b, binary.BigEndian, &buf)
	if asInt {
		return strconv.Itoa(length), nil
	}
	return string(buf), nil
}

// decode and read the length
func readLength(b *bytes.Reader) (length int, err error, asInt bool) {
	prefix, err := b.ReadByte()
	if err != nil {
		return 0, err, false
	}

	switch prefix >> 6 {
	case 0x00:
		return int(prefix & 0x3F), nil, false

	case 0x01:
		next, _ := b.ReadByte()
		return int(uint16(prefix&0x3F)<<8 | uint16(next)), nil, false

	case 0x02:
		var l uint32
		binary.Read(b, binary.BigEndian, &l)
		return int(l), nil, true

	case 0x03:
		switch prefix {
		case 0xC0:
			var l uint8
			binary.Read(b, binary.BigEndian, &l)
			return int(l), nil, true

		case 0xC1:
			var l uint16
			binary.Read(b, binary.BigEndian, &l)
			return int(l), nil, true

		case 0xC2:
			var l uint32
			binary.Read(b, binary.BigEndian, &l)
			return int(l), nil, true
		default:
			return int(prefix), nil, false
		}
	default:
		return 0, fmt.Errorf("unsupported length encoding"), false
	}
}

// return keys value from file and key with their expiration time
func readRDBintoStore(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	fmt.Println(data)

	r := bytes.NewReader(data)
	fbFlag := false
	expiry := time.Time{}

	for {
		op, err := r.ReadByte()
		if err != nil {
			break
		}

		if op == 0xFB {
			fbFlag = true
		}
		if !fbFlag {
			continue
		}

		switch op {

		case 0xFB:
			readLength(r)
			readLength(r)
		case 0xFD: //read 4 bytes, seconds expiration
			var expirySeconds uint32
			binary.Read(r, binary.LittleEndian, &expirySeconds)
			expiry = time.Unix(int64(expirySeconds), 0)
		case 0xFC: //read 8 bytes, milliseconds expiration
			var expiryMiliSec uint64
			binary.Read(r, binary.LittleEndian, &expiryMiliSec)
			expiry = time.Unix(0, int64(expiryMiliSec)*1_000_000)
		case 0x00:
			key, err := readString(r)
			if err != nil {
				fmt.Println("bad Key")
			}

			val, err := readString(r)
			if err != nil {
				fmt.Println("bad value")
			}
			if expiry.IsZero() {
				Store[key] = StoreValue{value: val, expiry: nil}
			} else {
				valToSave := expiry
				Store[key] = StoreValue{value: val, expiry: &valToSave}
			}
			expiry = time.Time{}
		case 0xFF:
			return nil
		}
	}
	return nil
}

func handleReplication(replicaof, port string) {
	role = "slave"
	response := make([]byte, 1024)

	parts := strings.Split(replicaof, " ")
	if len(parts) != 2 {
		fmt.Println("invalid replica format")
		return
	}
	masterHost, masterPort := parts[0], parts[1]
	conn, err := net.Dial("tcp", masterHost+":"+masterPort)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer conn.Close()

	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		fmt.Printf("Failed to send PING command to master at %s: %v\\n\"", masterPort, err)
	}

	if err != nil {
		fmt.Println("failed to read from connection ", err)
		return
	}

	//read response
	n, err := conn.Read(response)
	if err != nil {
		fmt.Printf("Failed to read PING response: %v\n", err)
		return
	}
	fmt.Printf("PING response: %s", string(response[:n]))

	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	if err != nil {
		fmt.Printf("Failed to send REPLCONF command: %v\n", err)
		return
	}

	n, err = conn.Read(response)
	if err != nil {
		fmt.Printf("Failed to read REPLCONF response: %v\n", err)
		return
	}
	fmt.Printf("REPLCONF response: %s", string(response[:n]))

	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Printf("Failed to send REPLCONF capa command to master at %s: %v\n", masterPort, err)
		return
	}

	n, err = conn.Read(response)
	if err != nil {
		fmt.Printf("Failed to read REPLCONF capa response: %v\n", err)
		return
	}
	fmt.Printf("REPLCONF capa response: %s", string(response[:n]))

	_, err = conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		fmt.Printf("Failed to send PSYNC command to master at %s: %v\n", masterPort, err)
		return
	}

	_, err = conn.Read(response)
	if err != nil {
		fmt.Printf("Failed to read PSYNC response: %v\n", err)
		return
	}
	fmt.Printf("PSYNC response: %s", string(response[:n]))

	for {
		n, err := conn.Read(response)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Master connection closed")
				break
			}
			fmt.Printf("error reading from master: %v\n", err)
		}
		if n > 0 {
			fmt.Printf("recieved from master: %s", string(response[:n]))
		}
	}
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	var dir = flag.String("dir", "", "Directory where RDB file is stored")
	var dbfilename = flag.String("dbfilename", "", "Name of the RDB file")
	var port = flag.Int("port", 6379, "port to listen on")
	var replicaof = flag.String("replicaof", "", "host and port of the master to replicate")

	flag.Parse()

	filePath := path.Join(*dir, *dbfilename)

	if *replicaof != "" {
		handleReplication(*replicaof, strconv.Itoa(*port))
	}

	err := readRDBintoStore(filePath)
	if err != nil {
		fmt.Println("failed to load RDB file")
	}

	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handleConnection(conn, *dir, *dbfilename)
	}
}
