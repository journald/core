package http

import (
	"encoding/json"
	"net/http"

	"github.com/journald/lsmtree"
)

type QueryMetaResponse struct {
	Key string `json:"key"`
}

type QueryResponse struct {
	Meta QueryMetaResponse `json:"meta"`
	Data string            `json:"query"`
}

func query(tree *lsmtree.LSMTree) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			WriteError(w, "405", "Method not allowed")
			return
		}

		key, err := ExtractParam(r.URL.String())
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteError(w, "400", "Missing URL param")
			return
		}

		value, err := tree.Get([]byte(key))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			WriteError(w, "404", "Key not found")
			return
		}

		err = json.NewEncoder(w).Encode(QueryResponse{
			Meta: QueryMetaResponse{
				Key: string(key),
			},
			Data: string(value),
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteError(w, "500", "Error while generating response payload")
			return
		}
	}
}
