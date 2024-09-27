package gateway

import "encoding/json"

// ConvertData ...
func ConvertData[T any](data []byte) T {
	var (
		r T
	)
	_ = json.Unmarshal(data, &r)
	return r
}

// ToBytes ...
func ToBytes(data interface{}) []byte {
	b, _ := json.Marshal(data)
	return b
}
