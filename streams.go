package loge

type Stream map[string]string

type Value [2]string
type Values []Value

type Entry struct {
	Stream Stream `json:"stream"`
	Values Values `json:"values"`
}

type Entries []Entry

type Payload struct {
	Streams Entries `json:"streams"`
}
