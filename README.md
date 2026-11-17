GOOS=windows GOARCH=amd64 go build sender.go

GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build \
  -trimpath \
  -ldflags="-compressdwarf=false" \
  -o fileSender.exe . 