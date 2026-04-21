package common

import "fmt"

func CreateQueueName(prefix string, id int) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}
