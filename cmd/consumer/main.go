package main

import (
	"context"
	"log"
	"time"

	"github.com/roh4nyh/go_rabbitmq/internal"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("rohan", "2486", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := client.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	messageBus, err := client.Consume(queue.Name, "email-service", false)
	if err != nil {
		panic(err)
	}

	var blocking chan struct{}

	// go func() {
	// 	for message := range messageBus {
	// 		log.Printf("New Message: %v\n", message)

	// 		// if err := message.Ack(false); err != nil {
	// 		// 	log.Println("Acknowlede message failed...")
	// 		// 	continue
	// 		// }
	// 		// panic("failed...")

	// 		if !message.Redelivered {
	// 			message.Nack(false, true)
	// 			continue
	// 		}

	// 		if err := message.Ack(false); err != nil {
	// 			log.Println("Acknowlede message failed...")
	// 			continue
	// 		}

	// 		log.Printf("Acknowledge message %s\n", message.MessageId)
	// 	}
	// }()

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	g, _ := errgroup.WithContext(ctx)

	// arrow groups allows us to set concurrent tasks
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			// spawn a worker...
			msg := message
			g.Go(func() error {
				log.Printf("New Message: %v", msg)
				time.Sleep(10 * time.Second)

				if err := msg.Ack(false); err != nil {
					log.Println("Ack message failed")
					return err
				}

				log.Printf("Acknowledged message: %s\n", msg.MessageId)
				return nil
			})
		}
	}()

	log.Println("Consuming, to close the program press CTRL+C")

	<-blocking
}
