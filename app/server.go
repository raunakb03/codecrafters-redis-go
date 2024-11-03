package main

import (
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var _ = net.Listen
var _ = os.Exit

func handleError(msg string, err error) {
    log.Fatalf("%s : %s", msg, err)
}

func Ping(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func parseRequest(conn io.Reader) ([]string, error) {
	msg := make([]byte, 1024)
	value := []string{}
	msglen, err := conn.Read(msg)
	if err != nil {
        handleError("Error reading from connection: ", err)
	}
	message := strings.TrimSpace(string(msg[:msglen]))
	messageSlice := strings.Split(message, "\r\n")
	for i, item := range messageSlice[2:] {
		if i%2 == 0 {
			value = append(value, item)
		}
	}
	return value, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
    decodedRequest, _ := parseRequest(conn)
    res := ""
    for _, item := range decodedRequest {
        if item=="ECHO" {
            continue;
        }
        res+= "$"
        res+= strconv.Itoa(len(item))
        res+= "\r\n"
        res+= item
        res+= "\r\n"
    }
	conn.Write([]byte(res))
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
