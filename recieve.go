package main

import (
	"github.com/streadway/amqp"
	"log"
)

func initChannel() *amqp.Channel {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Println(err)
	}
	return ch
}

func recv1() {
	// 1. 偷懒地建立连接
	ch := initChannel()
	// 1+ 这里也可以声明队列，因为我们可能会让接收方比发送方先运行
	// 2. 监听一个队列
	msgs, err := ch.Consume(
		"hello", // 刚才设定的队列名称
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
	}
	// 3. 循环处理队列消息
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
	}
}

func main() {
	recv1()
}
