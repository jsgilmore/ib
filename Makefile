install:
	go install .
	
regenerate:
	set -xe
	GOARCH=386 go tool cgo -godefs syscall_linux.go |gofmt -s >syscall_linux_386.go
	GOARCH=amd64 go tool cgo -godefs syscall_linux.go |gofmt -s >syscall_linux_amd64.go
	go install .

.DEFAULT_GOAL:=install