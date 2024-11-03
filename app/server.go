package main

import (
	"fmt"
	"log"
	"net"
	"os"
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

func Echo(str string, conn net.Conn) {
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)))
}

func handleParsing(conn net.Conn) []string {
	buf := make([]byte, 1024)
	buflen, err := conn.Read(buf)
	if err != nil {
		handleError("Error reading from connection: ", err)
	}
	message := strings.TrimSpace(string(buf[:buflen]))
	messageSlice := strings.Split(message, "\r\n")
	values := []string{}
	for i, item := range messageSlice {
		if i==0 || i&1==1 || item == "" || item == " " || item == "COMMAND" {
			continue
		}
        values = append(values, item)
	}
    return values
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
    decodedSlice := handleParsing(conn)
    for _, item := range(decodedSlice) {
        if item == "PING" {
            Ping(conn)
        } else if item == "ECHO" {
            continue;
        } else {
            Echo(item, conn)
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
