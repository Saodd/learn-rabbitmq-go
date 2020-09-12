package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"os"
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

func send10() {
	ch := initChannel()
	n, _ := strconv.Atoi(os.Args[1]) // 读取斐波那契函数参数，忽略异常
	q, _ := ch.QueueDeclare(         // 声明一个回调队列
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	msgs, _ := ch.Consume( // 在发送请求之前，先监听回调队列
		q.Name, // queue
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	corrId := uuid.New().String() // 生成一个随机id
	ch.Publish(
		"",
		"rpc_queue",
		false,
		false,
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId, // 指定任务id
			ReplyTo:       q.Name, // 指定回调队列
			Body:          []byte(strconv.Itoa(n)),
		})
	for msg := range msgs { // 处理回调消息
		if corrId == msg.CorrelationId {
			log.Println("收到回调：", string(msg.Body))
			break
		}
	}
}

func send12() {
	ch := initChannel()
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation)) // 监听发布确认结果
	ch.Confirm(false)                                          // 对当前Channel开启监听发布确认
	for i := 1; i <= 10; i++ {
		ch.Publish(
			"",
			"5-durable",
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("sleep %d 序号: %d", rand.Intn(10), i)),
			})
		log.Println("确认一条消息", i, <-confirms)
	}
}

func main() {
	send12()
}
