package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	return NewRabbitMQQueue(queueName, connectionSettings)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	return NewRabbitMQExchange(exchange, keys, connectionSettings)
}
