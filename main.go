package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"

	"github.com/JackSL/cestus/grpc"
	"github.com/sirupsen/logrus"
)


var IEEETable = crc32.MakeTable(crc32.IEEE)

func init() {
	// Initialize the application
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,

		TimestampFormat: "2006-01-02 15:04:05",
		DisableQuote:     true,
	})

	logrus.SetLevel(logrus.DebugLevel)

}

type Namespace struct {
	URI string
	Data map[string][]byte
	CRCVals map[string]uint32
}

func NewNamespace(uri string) *Namespace {
	return &Namespace{
		URI:  uri,
		Data: make(map[string][]byte),
		CRCVals: make(map[string]uint32),
	}
}

func (ns *Namespace) Add(key string, value []byte) {
	ns.Data[key] = value
	ns.CRCVals[key] = crc32.Checksum(value, IEEETable)
}

func (ns *Namespace) Get(key string) ([]byte, bool) {
	value, exists := ns.Data[key]
	if exists{
		crc := crc32.Checksum(value, IEEETable)
		if crc != ns.CRCVals[key] {
			logrus.Warnf("CRC mismatch for key %s: expected %d, got %d", key, ns.CRCVals[key], crc)
			return nil, false
		}
	}
	return value, exists
}

func (ns *Namespace) Delete(key string) {
	delete(ns.Data, key)
	delete(ns.CRCVals, key)
}

//type CestusServer interface {
//	Get(context.Context, *Request) (*Response, error)
//	Set(context.Context, *Request) (*Result, error)
//	Delete(context.Context, *Request) (*Result, error)
//	mustEmbedUnimplementedCestusServer()                      
//}

type CestusServerImpl struct {
	grpc.UnimplementedCestusServer
	Host string
	Namespaces map[string]*Namespace
}

func NewCestusServer(host string) *CestusServerImpl {
	return &CestusServerImpl{
		Host: host,
		Namespaces: make(map[string]*Namespace),
	}
}


func (s *CestusServerImpl) Get(ctx context.Context, req *grpc.Request) (*grpc.Response, error) {
	// Check if the namespace exists
	ns, exists := s.Namespaces[req.Namespace]
	if !exists {
		return nil, fmt.Errorf("namespace %s does not exist", req.Namespace)
	}

	// Get the value from the namespace
	value, exists := ns.Get(req.Key)
	if !exists {
		return nil, fmt.Errorf("key %s does not exist in namespace %s", req.Key, req.Namespace)
	}

	return &grpc.Response{Value: value}, nil
}

func (s *CestusServerImpl) Set(ctx context.Context, data *grpc.Data) (*grpc.Result, error) {
	// Check if the namespace exists
	ns, exists := s.Namespaces[data.Namespace]
	if !exists {
		return nil, fmt.Errorf("namespace %s does not exist", data.Namespace)
	}

	// Set the value in the namespace
	ns.Add(data.Key, data.Value)

	return &grpc.Result{Success: true}, nil
}

func (s *CestusServerImpl) Delete(ctx context.Context, req *grpc.Request) (*grpc.Result, error) {
	// Check if the namespace exists
	ns, exists := s.Namespaces[req.Namespace]
	if !exists {
		return nil, fmt.Errorf("namespace %s does not exist", req.Namespace)
	}

	// Delete the key from the namespace
	ns.Delete(req.Key)

	return &grpc.Result{Success: true}, nil
}

func main(){
	// create a new Listener 
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err
	}
	grpcServer := grpc.NewNewCestusServer("localhost:50051")

	grpc.RegisterCestusServer(grpcServer, &CestusServerImpl{})

	grpcServer.Serve(lis)

}
