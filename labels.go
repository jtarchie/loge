package loge

//go:generate msgp -tests=false
type LabelResponse struct {
	Status string   `json:"status" msg:"status"`
	Data   []string `json:"data"   msg:"data"`
}
