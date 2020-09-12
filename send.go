package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
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

func send2() {
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

func send3() {
	ch := initChannel()
	for i := 1; i <= 5; i++ { // 连续发5个任务
		ch.Publish(
			"",
			"3-sleep",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("sleep 2 " + "序号: " + strconv.Itoa(i)), // 指定睡眠任务执行时间
			})
	}
}

func send5() {
	ch := initChannel()
	ch.QueueDeclare(
		"5-durable",
		true, // 这里指定队列持久化
		false,
		false,
		false,
		nil,
	)
	for i := 1; i <= 5; i++ {
		ch.Publish(
			"",
			"5-durable",
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent, // 这里指定消息持久化
				ContentType:  "text/plain",
				Body:         []byte("sleep 2 " + "序号: " + strconv.Itoa(i)),
			})
	}
}

func send6() {
	ch := initChannel()
	for i := 1; i <= 10; i++ { // 发10个任务
		ch.Publish(
			"",
			"5-durable",
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("sleep %d 序号: %d", rand.Intn(10), i)), // 随机时间
			})
	}
}

func send7() {
	ch := initChannel()
	ch.ExchangeDeclare(
		"logs",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	for range time.Tick(time.Second) { // 每秒发送一条消息
		ch.Publish(
			"logs", // 注意这里指定了exchange 并清空了routing-key
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(fmt.Sprintf("【%s】 一些日志内容……", time.Now().String())),
			})
	}
}

func send8() {
	ch := initChannel()
	ch.ExchangeDeclare(
		"logs_direct",
		"direct", // 改变交换器类型
		true,
		false,
		false,
		false,
		nil,
	)
	keyMap := map[int]string{0: "black", 1: "green", 2: "orange"}
	for range time.Tick(time.Second) { // 每秒发送一条消息
		key := keyMap[rand.Intn(3)] // 随机关键字
		body := fmt.Sprintf("【%s】 一些日志内容……", time.Now().String())
		fmt.Println(key, body)
		ch.Publish(
			"logs_direct",
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
	}
}

func send9() {
	ch := initChannel()
	ch.ExchangeDeclare(
		"logs_topic",
		"topic", // 改变交换器类型
		true,
		false,
		false,
		false,
		nil,
	)
	facilityMap := map[int]string{0: "server0", 1: "server1", 2: "server2"}
	severityMap := map[int]string{0: "error", 1: "warning", 2: "info"}
	for range time.Tick(time.Millisecond * 100) { // 加快速度每0.1秒发送一条消息
		key := facilityMap[rand.Intn(3)] + "." + severityMap[rand.Intn(3)] // 随机关键字
		body := fmt.Sprintf("【%s】[%s] 一些日志内容……", time.Now().String(), key)
		fmt.Println(body)
		ch.Publish(
			"logs_topic",
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
	}
}

func main() {
	send9()
}
