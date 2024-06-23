package internal

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	// conneciton used by the client
	conn *amqp.Connection

	// channel is used to process / send messages
	ch *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vhost string) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}

	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, nil
	}

	return RabbitClient{
		conn: conn,
		ch:   ch,
	}, nil
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) (amqp.Queue, error) {
	q, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, err
	}
	return q, nil
}

// binding the current channel to the given exchange using routing key provided....
func (rc RabbitClient) CreateBinding(name, binding, exahnge string) error {
	// having 'noWait' set to false will make the channel return an error if it fails to bind...
	return rc.ch.QueueBind(name, binding, exahnge, false, nil)
}

// wraper function to publish payloads onto exchange with given routingKey...
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(ctx,
		exchange,
		routingKey,
		// used to determine if the error should be returned or not...
		true,
		// immediate
		false,
		options,
	)

	if err != nil {
		return err
	}

	log.Println(confirmation.Wait())
	return nil
}

// used to consume a queue
func (rc RabbitClient) Consume(queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, autoAck, false, false, false, nil)
}

// always use only one connection per service and don't use the conn-lose the connections that result in multiple tcp connections, might slow down your application
// spawn the multiple channels insted for concurrent tasks...
