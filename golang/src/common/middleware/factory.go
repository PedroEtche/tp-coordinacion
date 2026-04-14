package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	queue, err := NewQueue(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}

	return queue, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	exc, err := NewExchange(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}

	return exc, nil
}
