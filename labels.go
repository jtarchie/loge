package loge

//go:generate go run github.com/tinylib/msgp -tests=false
type LabelResponse struct {
	Status string   `json:"status" msg:"status"`
	Data   []string `json:"data"   msg:"data"`
}
