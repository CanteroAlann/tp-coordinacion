package middleware

import (
	"errors"
)

var (
	ErrRabbitMQCreateQueueMiddleware    = errors.New("error creating RabbitMQ queue middleware")
	ErrRabbitMQCreateChannel            = errors.New("error creating RabbitMQ channel")
	ErrRabbitMQDeclareQueue             = errors.New("error declaring RabbitMQ queue")
	ErrRabbitMQCreateExchangeMiddleware = errors.New("error creating RabbitMQ exchange middleware")
	ErrRabbitMQDeclareExchange          = errors.New("error declaring RabbitMQ exchange")
	ErrRabbitMQCreatingConnection       = errors.New("error creating RabbitMQ connection")
)
