


all: build

build: generate
	@echo "Cleaning..."
	@rm -rf bin/*
	@echo "Building..."
	@go build -o bin/cestus


generate: 
	@rm -rf grpc/*
	@echo "Generating files..."
	@protoc --go_out=. --go-grpc_out=. ./proto/*.proto

