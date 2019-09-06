package main

import (
	"github.com/Shopify/sarama"

	"testing"
	//"fmt"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

func captureOutput(f func() error) (string, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	stdout := os.Stdout
	stderr := os.Stderr
	defer func() {
		os.Stdout = stdout
		os.Stderr = stderr
		log.SetOutput(os.Stderr)
	}()
	os.Stdout = writer
	os.Stderr = writer
	log.SetOutput(writer)
	out := make(chan string)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		var buf bytes.Buffer
		wg.Done()
		io.Copy(&buf, reader)
		out <- buf.String()
	}()
	wg.Wait()
	err = f()
	writer.Close()
	return <-out, err
}

func TestConnectToKafkaCluster(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})
	//fmt.Printf("Addr %v\n",seedBroker.Addr())

	protocol = "plaintext"
	broker = seedBroker.Addr()
	_, err := connectToKafkaCluster()

	if err != nil {
		t.Fatal("Failed to connect to Kafka cluster: " + err.Error())
	}
}

func TestDumpSpec(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 2)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader("my_topic", 0, seedBroker.BrokerID()),
		"DescribeAclsRequest":    sarama.NewMockListAclsResponse(t),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	protocol = "plaintext"
	broker = seedBroker.Addr()
	out, err := captureOutput(func() error { return dumpSpec() })

	if err != nil {
		t.Fatal("Failed to dump spec: " + err.Error())
	}

	expected, err := ioutil.ReadFile("testdata/dump_spec.yaml")
	if err != nil {
		t.Fatal("Failed to read testdata/dump_spec.yaml: " + err.Error())
	}

	if out != string(expected) {
		t.Fatalf("Output:\n%s\nExpected:\n%s", out, expected)
	}
}

func TestApplySpecFile(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 2)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader("my_topic", 0, seedBroker.BrokerID()),
		"DescribeAclsRequest":     sarama.NewMockListAclsResponse(t),
		"DescribeConfigsRequest":  sarama.NewMockDescribeConfigsResponse(t),
		"CreateAclsRequest":       sarama.NewMockCreateAclsResponse(t),
		"CreateTopicsRequest":     sarama.NewMockCreateTopicsResponse(t),
		"AlterConfigsRequest":     sarama.NewMockAlterConfigsResponse(t),
		"CreatePartitionsRequest": sarama.NewMockCreatePartitionsResponse(t),
	})

	protocol = "plaintext"
	broker = seedBroker.Addr()
	specfile = "testdata/apply_spec.yaml"
	out, err := captureOutput(func() error { return applySpecFile() })

	if err != nil {
		t.Fatal("Failed to apply spec: " + err.Error())
	}

	expected := Ok + " ok=2   " + Default + Changed + " changed=8   " + Default + " failed=0\n" + Default

	if !strings.Contains(out, expected) {
		t.Fatalf("Output does not contain expected \"%s\":\n%s", expected, out)
	}
	//fmt.Printf("Apply output: %s\n", out)
}

func TestHello(t *testing.T) {
	host := getHost("test:*")

	if host != "*" {
		t.Errorf("getHost failed, expected %s, got %s", "*", host)
	}
}
