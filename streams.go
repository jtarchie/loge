package loge

type Stream map[string]string

type Value [2]string
type Values []Value

type Entry struct {
	Stream Stream `json:"stream" msg:"stream"`
	Values Values `json:"values" msg:"values"`
}

type Entries []Entry

//go:generate go run github.com/tinylib/msgp -tests=false
type Payload struct {
	Streams Entries `json:"streams" msg:"streams"`
}
