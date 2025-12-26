package loge

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/goccy/go-json"
	pb "github.com/jtarchie/loge/proto"
	"github.com/labstack/echo/v4"
	"github.com/tinylib/msgp/msgp"
	"google.golang.org/protobuf/proto"
)

const (
	msgpackContentType  = "application/msgpack"
	protobufContentType = "application/protobuf"
)

func bind(context echo.Context, payload interface{}) error {
	contentType := context.Request().Header.Get(echo.HeaderContentType)

	switch {
	case strings.Contains(contentType, protobufContentType):
		p, ok := payload.(*Payload)
		if !ok {
			return errors.New("could not convert payload to protobuf target")
		}

		body, err := io.ReadAll(context.Request().Body)
		if err != nil {
			return fmt.Errorf("could not read protobuf body: %w", err)
		}

		pbPayload := &pb.Payload{}
		if err := proto.Unmarshal(body, pbPayload); err != nil {
			return fmt.Errorf("could not unmarshal protobuf: %w", err)
		}

		// Convert protobuf payload to internal Payload type
		p.Streams = make(Streams, 0, len(pbPayload.Streams))
		for _, stream := range pbPayload.Streams {
			entry := Entry{
				Stream: make(Stream, len(stream.Stream)),
				Values: make(Values, 0, len(stream.Values)),
			}
			for k, v := range stream.Stream {
				entry.Stream[k] = v
			}
			for _, val := range stream.Values {
				entry.Values = append(entry.Values, Value{val.Timestamp, val.Line})
			}
			p.Streams = append(p.Streams, entry)
		}

	case strings.Contains(contentType, msgpackContentType):
		decodable, ok := payload.(msgp.Decodable)
		if !ok {
			return errors.New("could not convert payload to msgpack")
		}

		err := msgp.Decode(context.Request().Body, decodable)
		if err != nil {
			return fmt.Errorf("could not unmarshal msgpack: %w", err)
		}
	case strings.Contains(contentType, "application/json"):
		err := json.NewDecoder(context.Request().Body).Decode(payload)
		if err != nil {
			return fmt.Errorf("could not unmarshal json: %w", err)
		}
	default:
		return errors.New("could not read streams")
	}

	return nil
}

func response(context echo.Context, status int, payload interface{}) error {
	accept := context.Request().Header.Get(echo.HeaderAccept)

	if strings.Contains(accept, msgpackContentType) {
		encodable, ok := payload.(msgp.Encodable)
		if !ok {
			return errors.New("could not convert payload to msgpack")
		}

		context.Response().Header().Set(echo.HeaderContentType, msgpackContentType)
		context.Response().WriteHeader(status)

		err := msgp.Encode(context.Response(), encodable)
		if err != nil {
			return fmt.Errorf("could not marshal payload to msgpack: %w", err)
		}

		return nil
	}

	return context.JSON(status, payload)
}
