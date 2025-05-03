package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"time"
	"github.com/JackSL/cestus/grpc"
	"github.com/sirupsen/logrus"
	pb "google.golang.org/grpc"
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
//	Delete(context.Context, *Request) (*Result, error)a
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
		logrus.Infof("Creating new namespace %s", data.Namespace)
		ns = NewNamespace(data.Namespace)
		s.Namespaces[data.Namespace] = ns
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

type CestusClientImpl struct {
	Host string
}

func NewCestusClient(host string) *CestusClientImpl {
	return &CestusClientImpl{
		Host: host,
	}
}

func (c *CestusClientImpl) Get(ctx context.Context, req *grpc.Request) (*grpc.Response, error) {
	conn, err := pb.Dial(c.Host, pb.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := grpc.NewCestusClient(conn)

	// Call the Get method on the server
	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get value: %v", err)
	}

	return resp, nil
}

func (c *CestusClientImpl) Set(ctx context.Context, data *grpc.Data) (*grpc.Result, error) {
	conn, err := pb.Dial(c.Host, pb.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := grpc.NewCestusClient(conn)

	// Call the Set method on the server
	resp, err := client.Set(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("failed to set value: %v", err)
	}

	return resp, nil
}

func (c *CestusClientImpl) Delete(ctx context.Context, req *grpc.Request) (*grpc.Result, error) {
	conn, err := pb.Dial(c.Host, pb.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := grpc.NewCestusClient(conn)

	// Call the Delete method on the server
	resp, err := client.Delete(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to delete value: %v", err)
	}

	return resp, nil
}

func RunTests() {
	client := NewCestusClient("localhost:50051")
	ctx := context.Background()
	// Create a new namespace
	namespace := "test_namespace"
	// Create a new key-value pair
	key := "test_key"
	value := []byte("test_value")

	// Set the key-value pair in the namespace
	data := &grpc.Data{
		Namespace: namespace,
		Key:       key,
		Value:     value,
	}

	result, err := client.Set(ctx, data)

	if err != nil {
		logrus.Errorf("Failed to set value: %v", err)
		return
	}

	if !result.Success {
		logrus.Errorf("Failed to set value: %v", result)
		return
	}

	// Get the value from the namespace
	req := &grpc.Request{
		Namespace: namespace,
		Key:       key,
	}

	resp, err := client.Get(ctx, req)
	if err != nil {
		logrus.Errorf("Failed to get value: %v", err)
		return
	}

	if resp.Value == nil {
		logrus.Errorf("Failed to get value: %v", resp)
		return
	}

	// Check if the value matches
	if string(resp.Value) != string(value) {
		logrus.Errorf("Value mismatch: expected %s, got %s", value, resp.Value)
		return
	}

	logrus.Infof("Successfully set and got value: %s", resp.Value)
	// Delete the key from the namespace
	deleteReq := &grpc.Request{
		Namespace: namespace,
		Key:       key,
	}

	deleteResult, err := client.Delete(ctx, deleteReq)
	if err != nil {
		logrus.Errorf("Failed to delete value: %v", err)
		return
	}

	if !deleteResult.Success {
		logrus.Errorf("Failed to delete value: %v", deleteResult)
		return
	}


}


func main(){
	// create a new Listener 
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}
	grpcServer := pb.NewServer()


	grpc.RegisterCestusServer(grpcServer, &CestusServerImpl{})

	// Register the CestusServerImpl with the gRPC server
	logrus.Info("Starting gRPC server on port 50051")
	go func(){
		time.Sleep(2 * time.Second)
		RunTests()
	}()
	// Start the gRPC server
	err = grpcServer.Serve(lis)
	if err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
	// Run tests


}
