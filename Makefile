.PHONY: journald
journald:
	go install github.com/journald/cmd/journald

test:
	go test ./...
