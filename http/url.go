package http

import (
	"fmt"
	"strings"
)

func ExtractParam(url string) (string, error) {
	r := strings.Split(url, "/")
	if len(r) < 2 {
		return "", fmt.Errorf("Can't extract param from URL")
	}
	return r[2], nil
}
