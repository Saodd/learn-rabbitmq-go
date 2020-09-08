package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func sender1() {
	// 1. 建立连接
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		log.Println(err)
	}
	defer ch.Close()

	// 2. 声明队列
	q, err := ch.QueueDeclare(
		"hello", // 队列名称
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
	}

	// 3. 发布消息
	err = ch.Publish(
		"",
		q.Name, // 要发往的队列名称
		false,
		false,
		amqp.Publishing{ // 消息结构体
			ContentType: "text/plain",
			Body:        []byte("Hello World!" + fmt.Sprint(time.Now().String())),
		})
	if err != nil {
		log.Println(err)
	}
}

func main() {
	sender1()
}
