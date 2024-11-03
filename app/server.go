package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var _ = net.Listen
var _ = os.Exit

var redisMap = make(map[string]string)

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
            if len(values) == 3 {
                redisMap[values[1]] = values[2]
                conn.Write([]byte("+OK\r\n"))
            } else {
                conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
            }
        case "GET":
            if len(values) == 2 {
                value, ok := redisMap[values[1]]
                if ok {
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
