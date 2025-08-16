package hub

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	msg "github.com/WebmiRU/Yudole_multichat-msg"
)

const RECONNECT_TIMEOUT = 5 * time.Second

var outChan = make(chan any, 9999)
var InChan = make(chan msg.Event, 9999)
var NetStateChan = make(chan string, 999)

func Connect(host, port string) {
	ctx, cancel := context.WithCancel(context.Background()) // Контекст для отмены отправки данных в сокет Хаба
	defer reconnect(cancel, host, port)

	var conn, err = net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))

	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	if err != nil {
		log.Printf("Could not connect to HUB: %s\n", err)
		return
	}

	NetStateChan <- "connected"

	fmt.Println("HUB connection established")

	go writer(ctx, conn)
	reader(conn)
}

func Send(message any) {
	outChan <- message
}

func JoinSuccess(service, channel string) {
	outChan <- msg.Event{
		Type:    "chat.channel.join.success",
		Service: service,
		Channel: channel,
	}
}

func JoinFailed(service, channel string) {
	outChan <- msg.Event{
		Type:    "chat.channel.join.failed",
		Service: service,
		Channel: channel,
	}
}

func reconnect(cancel context.CancelFunc, host, port string) {
	cancel()

	NetStateChan <- "reconnecting"

	log.Println("HUB connection lost. Reconnecting...")

	for len(InChan) > 0 {
		<-InChan
	}

	for len(outChan) > 0 {
		<-outChan
	}

	time.Sleep(RECONNECT_TIMEOUT)
	Connect(host, port)
}

func Event(event msg.EventPayload) {
	outChan <- event
}

func reader(conn net.Conn) {
	var dec = jsontext.NewDecoder(conn)

	for {
		var message msg.Event
		err := json.UnmarshalDecode(dec, &message)

		if err == io.EOF {
			conn.Close()
			return
		} else if err != nil {
			conn.Close()
			fmt.Println(err)
			return
		}

		fmt.Println("HUB_IN_CHAN:", message)
		InChan <- message
	}
}

func writer(ctx context.Context, conn net.Conn) {
	var enc = jsontext.NewEncoder(conn)

	for {
		select {
		case message := <-outChan:
			err := json.MarshalEncode(enc, message)

			if err != nil {
				conn.Close()
				log.Println(err) // Возможно вывод этой ошибки не требуется
				return
			}

		case <-ctx.Done():
			return
		}

	}
}
