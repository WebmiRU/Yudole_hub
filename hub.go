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

type Ability struct {
	Service string
	Value   string
}

var outChan = make(chan any, 9999)
var InChan = make(chan msg.Event, 9999)

var Connected bool
var Abilities []Ability // Массив задач которые решает клиент (абилки). Возможная проблема такого решения - будут приниматься и отзываться сразу все абилки, но, т.к. пока не планируется создавать в одном клиенте несколько независимых друг от друга набора абилок - такой вариант выглядит оптимальным на данный момент оптимальным
var isAccepted bool     // Если флаг установлен - после подключения/реконнекта отправляем список абилок на сервер

// Отправляем на сервер список абилок клиента
func Accept() {
	isAccepted = true

	for _, ability := range Abilities {
		outChan <- msg.EventPayload{
			Event: msg.Event{
				Type:    "ability.accept",
				Value:   ability.Value,
				Service: ability.Service,
			},
		}
	}
}

// Отправляем на сервер список абилок клиента (отключаем)
func Drop() {
	isAccepted = false

	for _, ability := range Abilities {
		outChan <- msg.EventPayload{
			Event: msg.Event{
				Type:    "ability.drop",
				Value:   ability.Value,
				Service: ability.Service,
			},
		}
	}
}

func Connect(host, port string) {
	defer reconnect(host, port)

	var conn, err = net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		log.Printf("Could not connect to HUB: %s\n", err)
		return
	}

	ctx, stop := context.WithCancel(context.Background()) // Контекст для отмены отправки данных в сокет Хаба
	defer conn.Close()                                    // Закрываем Сокет
	defer stop()                                          // Останавливаем Reader и Writer

	if isAccepted {
		Accept()
	}

	fmt.Printf("HUB connection established to: %s:%s\n", host, port)

	go writer(ctx, conn)
	reader(ctx, conn)
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

func reconnect(host, port string) {
	outChan = make(chan any, 9999)

	log.Println("HUB connection lost. Reconnecting...")

	time.Sleep(RECONNECT_TIMEOUT)
	Connect(host, port)
}

func Event(event msg.EventPayload) {
	outChan <- event
}

func reader(ctx context.Context, conn net.Conn) {
	var dec = jsontext.NewDecoder(conn)

	for {
		select {
		case <-ctx.Done(): // Завершаем Читателя
			log.Println("HUB Reader stopped")
			return

		default:
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

			fmt.Println("HUB incoming message:", message)
			InChan <- message
		}

	}
}

func writer(ctx context.Context, conn net.Conn) {
	var enc = jsontext.NewEncoder(conn)

	for {
		select {
		case message := <-outChan:
			err := json.MarshalEncode(enc, message)

			if err != nil { // Ошибка отправки сообщения в Сокет
				log.Println(err)
				conn.Close()
				return
			}

			log.Println("HUB out message:", message)

		case <-ctx.Done(): // Завершаем Писателя
			log.Println("HUB Writer stopped:", ctx.Err())
			return
		}

	}
}
