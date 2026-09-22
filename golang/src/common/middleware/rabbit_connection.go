package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQConnection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewRabbitMQConnection(hostname string, port int) (*RabbitMQConnection, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrRabbitMQCreatingConnection
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, ErrRabbitMQCreateChannel
	}

	return &RabbitMQConnection{
		conn: conn,
		ch:   ch,
	}, nil
}

func (r *RabbitMQConnection) GetChannel() *amqp.Channel {
	return r.ch
}

func (r *RabbitMQConnection) Close() error {
	if r.ch != nil && !r.ch.IsClosed() {
		if err := r.ch.Close(); err != nil && err != amqp.ErrClosed {
			return err
		}
	}

	if r.conn != nil && !r.conn.IsClosed() {
		if err := r.conn.Close(); err != nil && err != amqp.ErrClosed {
			return err
		}
	}

	return nil
}

func (r *RabbitMQConnection) IsClosed() bool {
	return r.conn == nil || r.conn.IsClosed()
}

func (r *RabbitMQConnection) StopConsuming(tag string) error {
	if r.ch != nil && !r.ch.IsClosed() {
		if err := r.ch.Cancel(tag, false); err != nil {
			return err
		}
	}
	return nil
}
