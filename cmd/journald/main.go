package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/journald/http"
	"github.com/journald/lsmtree"
)

func main() {
	dbDirectoryPtr := flag.String("db", "./data", "database directory")
	flag.Parse()

	tree, err := lsmtree.New(10000, *dbDirectoryPtr)
	if err != nil {
		log.Fatal(err)
	}

	if len(flag.Args()) < 1 {
		fmt.Println("print usage here")
	} else {
		if flag.Args()[0] == "put" {
			err = tree.Put([]byte(flag.Args()[1]), []byte(flag.Args()[2]))
			if err != nil {
				log.Fatal(err)
			}
		} else if flag.Args()[0] == "get" {
			data, err := tree.Get([]byte(flag.Args()[1]))
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s\n", data)
		} else if flag.Args()[0] == "scan" {
			if len(flag.Args()) == 1 {
				err = tree.ScanAll(func(key, value []byte) {
					fmt.Printf("%s | %s\n", key, value)
				})
			} else if len(flag.Args()) == 2 {
				key := []byte(flag.Args()[1])
				err = tree.Scan(key, func(key, value []byte) {
					fmt.Printf("%s | %s\n", key, value)
				})
			}

			if err != nil {
				log.Fatal(err)
			}
		} else if flag.Args()[0] == "http" {
			api, err := http.New(*dbDirectoryPtr)
			if err != nil {
				log.Fatal(err)
			}

			log.Fatal(api.ListenAndServe(":8080"))
		}
	}
}
