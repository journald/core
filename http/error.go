package http

import (
	"encoding/json"
	"io"
)

type ErrorMetaResponse struct {
	Code  string `json:"code"`
	Error string `json:"error"`
}

type ErrorResponse struct {
	Meta ErrorMetaResponse `json:"meta"`
}

func WriteError(w io.Writer, code, message string) {
	json.NewEncoder(w).Encode(ErrorResponse{
		Meta: ErrorMetaResponse{
			Code:  code,
			Error: message,
		},
	})
}
