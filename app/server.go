package main

import (
	"bufio"
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

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		values := handleParsing(conn)
		if len(values) == 0 {
			continue
		}
		command := strings.ToUpper(values[0])
		switch command {
		case "PING":
			Ping(conn)
		case "ECHO":
			if len(values) > 1 {
				Echo(values[1], conn)
			}
		case "SET":
			if len(values) != 3 || len(values) != 5 {
				redisMap[values[1]] = ValueType{
					key:                          values[1],
					value:                        values[2],
					hasExpiration:                false,
					expirationTimeInMilliseconds: 0,
				}
				if len(values) == 5 {
					val, _ := redisMap[values[1]]
					val.hasExpiration = true
					intTime, err := strconv.Atoi(values[4])
					if err != nil {
						handleError("Wrong time format ", err)
					}
					val.expirationTimeInMilliseconds = time.Now().UnixMilli() + int64(intTime)
                    redisMap[values[1]] = val
				}
				conn.Write([]byte("+OK\r\n"))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
			}
		case "GET":
			if len(values) == 2 {
				valueStruct, ok := redisMap[values[1]]
				if valueStruct.hasExpiration && valueStruct.expirationTimeInMilliseconds < time.Now().UnixMilli() {
					ok = false
				}
				if ok {
					value := valueStruct.value
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
				} else {
					conn.Write([]byte("$-1\r\n"))
				}
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
			}
		default:
			conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))
		}
	}
}

func main() {
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
