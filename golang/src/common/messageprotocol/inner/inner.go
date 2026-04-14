package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type innerMessage struct {
	QueryID  string        `json:"query_id"`
	ClientID string        `json:"client_id"`
	Records  []interface{} `json:"records"`
}

func deserializeJson(message []byte) (innerMessage, error) {
	var data innerMessage
	if err := json.Unmarshal(message, &data); err != nil {
		return innerMessage{}, err
	}
	return data, nil
}

func SerializeMessage(clientID string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	records := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		records = append(records, datum)
	}

	queryID := uuid.New().String()
	innerMessage := innerMessage{QueryID: queryID, ClientID: clientID, Records: records}

	body, err := json.Marshal(innerMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

// DeserializeMessage Retorna el id de la query, el id del cliente y un slice de FruitItems
func DeserializeMessage(message *middleware.Message) (string, string, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", "", nil, false, err
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range data.Records {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return "", "", nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return "", "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return "", "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return data.QueryID, data.ClientID, fruitRecords, len(fruitRecords) == 0, nil
}
