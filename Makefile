


all: generate

generate: 
	@echo "Generating files..."
	@protoc --go_out=. --go-grpc_out=. ./proto/*.proto

