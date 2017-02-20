package http

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/journald/lsmtree"
)

type CommandMetaResponse struct {
	Key string `json:"key"`
}

type CommandResponse struct {
	Meta CommandMetaResponse `json:"meta"`
	Data string              `json:"command"`
}

func command(tree *lsmtree.LSMTree) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			WriteError(w, "405", "Method not allowed")
			return
		}

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteError(w, "500", "Can't read request payload")
			return
		}

		key, err := ExtractParam(r.URL.String())
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteError(w, "400", "Missing URL param")
			return
		}

		err = tree.Put([]byte(key), body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteError(w, "500", fmt.Sprintf("Error while inserting payload in the log: %s", err))
			return
		}

		err = json.NewEncoder(w).Encode(CommandResponse{
			Meta: CommandMetaResponse{
				Key: string(key),
			},
			Data: string(body),
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteError(w, "500", "Error while generating response payload")
			return
		}
	}
}
