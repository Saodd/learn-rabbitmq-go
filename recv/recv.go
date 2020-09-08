package main

import (
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
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

func recv2() {
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

func recv3() {
	ch := initChannel()
	ch.QueueDeclare(
		"3-sleep", // 队列名称
		false,
		false,
		false,
		false,
		nil,
	)
	msgs, _ := ch.Consume(
		"3-sleep", // 刚才设定的队列名称
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	for msg := range msgs {
		var taskTimeStr = string(msg.Body[6:7])
		taskTime, _ := strconv.Atoi(taskTimeStr)
		log.Println("接受任务：", string(msg.Body))
		time.Sleep(time.Duration(taskTime) * time.Second)
		log.Println("完成任务")
	}
}

func recv3a() {
	ch := initChannel()
	ch.QueueDeclare(
		"3-sleep",
		false,
		false,
		false,
		false,
		nil,
	)
	msgs, _ := ch.Consume(
		"3-sleep",
		"",
		false, // 这里禁止自动签收
		false,
		false,
		false,
		nil,
	)
	for msg := range msgs {
		var taskTimeStr = string(msg.Body[6:7])
		taskTime, _ := strconv.Atoi(taskTimeStr)
		log.Println("接受任务：", string(msg.Body))
		time.Sleep(time.Duration(taskTime) * time.Second)
		log.Println("完成任务")
		msg.Ack(false) // 消息签收。注意参数是false
	}
}

func recv4() {
	ch := initChannel()
	msgs, _ := ch.Consume(
		"4-durable",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	for msg := range msgs {
		var taskTimeStr = string(msg.Body[6:7])
		taskTime, _ := strconv.Atoi(taskTimeStr)
		log.Println("接受任务：", string(msg.Body))
		time.Sleep(time.Duration(taskTime) * time.Second)
		log.Println("完成任务")
		msg.Ack(false)
	}
}

func recv5() {
	ch := initChannel()
	msgs, _ := ch.Consume(
		"5-durable",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	for msg := range msgs {
		var taskTimeStr = string(msg.Body[6:7])
		taskTime, _ := strconv.Atoi(taskTimeStr)
		log.Println("接受任务：", string(msg.Body))
		time.Sleep(time.Duration(taskTime) * time.Second)
		log.Println("完成任务")
		msg.Ack(false)
	}
}

func recv6() {
	ch := initChannel()
	ch.Qos(
		1, // 限制数量
		0,
		false,
	)
	msgs, _ := ch.Consume(
		"5-durable",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	for msg := range msgs {
		var taskTimeStr = string(msg.Body[6:7])
		taskTime, _ := strconv.Atoi(taskTimeStr)
		log.Println("接受任务：", string(msg.Body))
		time.Sleep(time.Duration(taskTime) * time.Second)
		log.Println("完成任务")
		msg.Ack(false)
	}
}

func main() {
	recv6()
}
