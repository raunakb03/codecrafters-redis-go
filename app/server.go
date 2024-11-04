package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var _ = net.Listen
var _ = os.Exit

type ValueType struct {
	key                          string
	value                        string
	hasExpiration                bool
	expirationTimeInMilliseconds int64
}

var redisMap = make(map[string]ValueType)

func handleError(msg string, err error) {
	log.Fatalf("%s : %s", msg, err)
}

func Ping(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func Echo(str string, conn net.Conn) {
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)))
}

func GetRespArray(values []string) string {
	respArrString := "*" + strconv.Itoa(len(values)) + "\r\n"
	for _, item := range values {
		respArrString += "$" + strconv.Itoa(len(item)) + "\r\n" + item + "\r\n"
	}
	return respArrString
}

func Set(key string, value string, isExpiration bool, expireAfter int64) {
	redisMap[key] = ValueType{
		key:                          key,
		value:                        value,
		hasExpiration:                isExpiration,
		expirationTimeInMilliseconds: time.Now().UnixMilli() + expireAfter,
	}
}

func Get(key string) string {
	valueStruct, ok := redisMap[key]
	if valueStruct.hasExpiration && valueStruct.expirationTimeInMilliseconds < time.Now().UnixMilli() {
		ok = false
	}
	if ok {
		return valueStruct.value
	}
	return ""
}

func handleParsing(conn net.Conn) []string {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		if err.Error() == "EOF" {
			return nil
		}
		handleError("Error reading from connection: ", err)
	}
	line = strings.TrimSpace(line)

	if line == "" {
		return nil
	}

	if line[0] != '*' {
		handleError("Invalid RESP format", fmt.Errorf("expected '*', got '%c'", line[0]))
	}

	numArgs, err := strconv.Atoi(line[1:])
	if err != nil {
		handleError("Invalid number of arguments", err)
	}

	values := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			handleError("Error reading from connection: ", err)
		}
		line = strings.TrimSpace(line)
		if line[0] != '$' {
			handleError("Invalid RESP format", fmt.Errorf("expected '$', got '%c'", line[0]))
		}

		argLen, err := strconv.Atoi(line[1:])
		if err != nil {
			handleError("Invalid argument length", err)
		}

		arg := make([]byte, argLen+2)
		_, err = reader.Read(arg)
		if err != nil {
			handleError("Error reading argument", err)
		}

		values[i] = string(arg[:argLen])
	}

	return values
}

func handleParsedValues(values []string, conn net.Conn, respArr bool) {
	command := strings.ToUpper(values[0])
	switch command {
	case "PING":
		Ping(conn)
	case "ECHO":
		if len(values) > 1 {
			Echo(values[1], conn)
		}
	case "SET":
		switch {
		case len(values) == 3:
			Set(values[1], values[2], false, 0)
			conn.Write([]byte("+OK\r\n"))
		case len(values) == 5:
			expireAfter, err := strconv.Atoi(values[4])
			if err != nil {
				handleError("Error converting expireAfter to int", err)
			}
			Set(values[1], values[2], true, int64(expireAfter))
			conn.Write([]byte("+OK\r\n"))
		default:
			conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		}
	case "GET":
		if len(values) == 2 {
			val := Get(values[1])
			if val != "" {
				if respArr {
                    respArrValues := []string{values[1], val}
                    conn.Write([]byte(GetRespArray(respArrValues)))
				} else {
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))

				}
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		}
	case "CONFIG":
		handleParsedValues(values[1:], conn, true)
	default:
		conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		values := handleParsing(conn)
		if len(values) == 0 {
			continue
		}
		handleParsedValues(values, conn, false)
	}
}

func main() {
	dbDir := flag.String("dir", "nil", "name of the RDB config file directory")
	dbFilename := flag.String("dbfilename", "nil", "name of RDB config file")

	flag.Parse()

	Set("dir", *dbDir, false, 0)
	Set("dbfilename", *dbFilename, false, 0)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		handleError("Error listening to port ", err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			handleError("Error accepting connection: ", err)
		}
		go handleConnection(conn)
	}
}
