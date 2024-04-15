package loge

type Stream map[string]string

type (
	Value  [2]string
	Values []Value
)

type Entry struct {
	Stream Stream `json:"stream" msg:"stream"`
	Values Values `json:"values" msg:"values"`
}

type Entries []Entry

//go:generate msgp -tests=false
type Payload struct {
	Streams Entries `json:"streams" msg:"streams"`
}
