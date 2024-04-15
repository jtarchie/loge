package loge

import (
	"errors"
	"fmt"
	"strings"

	"github.com/goccy/go-json"
	"github.com/labstack/echo/v4"
	"github.com/tinylib/msgp/msgp"
)

func bind(context echo.Context, payload interface{}) error {
	contentType := context.Request().Header.Get(echo.HeaderContentType)

	switch {
	case strings.Contains(contentType, "application/msgpack"):
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
