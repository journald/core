package http

import (
	"net/http"

	"github.com/journald/lsmtree"
)

type Api struct {
	*http.ServeMux
}

func New(path string) (Api, error) {
	tree, err := lsmtree.New(10, path)
	if err != nil {
		return Api{}, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/command/", trace(command(tree)))
	mux.HandleFunc("/query/", trace(query(tree)))

	return Api{
		ServeMux: mux,
	}, nil
}

func (a Api) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, a.ServeMux)
}
