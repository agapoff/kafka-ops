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
	"regexp"
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

	expected := [5]string{
		Ok + " ok=2   " + Default + Changed + " changed=8   " + Default + " failed=0\n" + Default,
		"[TOPIC : Modify topic my_topic (partitions=1)]",
		"[TOPIC : Create topic my_topic1 (partitions=3, replicas=1)]",
		"[TOPIC : Delete topic my_topic4]",
		"[ACL : Create ACL (ALLOW User:test1@* to READ topic:PREFIXED:my-)]",
	}

	for _, str := range expected {
		if !strings.Contains(out, str) {
			t.Fatalf("Output does not contain expected \"%s\":\n%s", str, out)
		}
	}
	//fmt.Printf("Apply output: %s\n", out)
}

func TestApplySpecFileTemplate(t *testing.T) {
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
		"DeleteTopicsRequest":     sarama.NewMockDeleteTopicsResponse(t),
	})

	broker = ""
	varFlags = append(varFlags, "Broker="+seedBroker.Addr())
	varFlags = append(varFlags, "Topic=my")
	isTemplate = true
	specfile = "testdata/apply_spec_template.yaml"
	out, err := captureOutput(func() error { return applySpecFile() })

	if err != nil {
		t.Fatal("Failed to apply spec: " + err.Error())
	}

	expected := [2]string{
		"[TOPIC : Delete topic my_topic]",
		"changed: [127.0.0.1:",
	}

	for _, str := range expected {
		if !strings.Contains(out, str) {
			t.Fatalf("Output does not contain expected \"%s\":\n%s", str, out)
		}
	}
}

func TestApplySpecFileAlterTopic(t *testing.T) {
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

	isYAML = true
	broker = seedBroker.Addr()
	specfile = "testdata/apply_spec_alter_topic.yaml"
	isTemplate = false
	verbose = true
	out, err := captureOutput(func() error { return applySpecFile() })

	if err != nil {
		t.Fatal("Failed to apply spec: " + err.Error())
	}

	expected := [2]string{
		"[TOPIC : Modify topic my_topic (partitions=2)]",
		"\"state\": \"present\"",
	}

	for _, str := range expected {
		if !strings.Contains(out, str) {
			t.Fatalf("Output does not contain expected \"%s\":\n%s", str, out)
		}
	}
}

func TestApplySpecFileDeleteAcl(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 2)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		//"SaslAuthenticateRequest": sarama.NewMockSaslAuthenticateResponse(t),
		//"SaslHandshakeRequest": sarama.NewMockSaslHandshakeResponse(t).
		//	SetEnabledMechanisms([]string{sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512}),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeAclsRequest":    sarama.NewMockListAclsResponse(t),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
		"DeleteAclsRequest":      sarama.NewMockDeleteAclsResponse(t),
	})

	isYAML = true
	broker = seedBroker.Addr()
	//protocol = "sasl_plain"
	mechanism = "scram-sha-256"
	username = "test"
	password = "test"
	specfile = "testdata/apply_spec_delete_acl.yaml"
	isTemplate = false
	verbose = false
	out, err := captureOutput(func() error { return applySpecFile() })
	//t.Fatalf("%v\n%v\n\n", out, err)
	if err != nil {
		t.Fatal("Failed to apply spec: " + err.Error())
	}

	expected := [2]string{
		"[ACL : Remove ACL (ALLOW User:test@* to IDEMPOTENT_WRITE cluster:LITERAL:kafka-cluster)]",
		"changed=1",
	}

	for _, str := range expected {
		if !strings.Contains(out, str) {
			t.Fatalf("Output does not contain expected \"%s\":\n%s", str, out)
		}
	}
}

func TestApplySpecFileDeleteByPattern(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 2)
	defer seedBroker.Close()

	group := "my_group"

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader("my_topic1", 0, seedBroker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
		"DeleteTopicsRequest":    sarama.NewMockDeleteTopicsResponse(t),
		"DeleteGroupsRequest":    sarama.NewMockDeleteGroupsRequest(t).SetDeletedGroups([]string{group}),
		"ListGroupsRequest":      sarama.NewMockListGroupsResponse(t).AddGroup(group, "consumer"),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).SetCoordinator(sarama.CoordinatorGroup, group, seedBroker),
	})

	protocol = "plaintext"
	broker = seedBroker.Addr()
	specfile = "testdata/apply_spec_delete_by_pattern.yaml"
	verbose = true
	out, err := captureOutput(func() error { return applySpecFile() })

	if err != nil {
		t.Fatal("Failed to apply spec: " + err.Error())
	}

	expected := [5]string{
		Ok + " ok=1   " + Default + Changed + " changed=5   " + Default + " failed=0\n" + Default,
		"\"matched\": [\n        \"my_topic1\"",
		"TASK [TOPIC : Delete topics prefixed by my_]",
		"TASK [CONSUMER-GROUP : Delete consumer-group match by grou[a-z]]",
		"\"matched\": [\n        \"my_group\"",
	}

	for _, str := range expected {
		if !strings.Contains(out, str) {
			t.Fatalf("Output does not contain expected \"%s\":\n%s", str, out)
		}
	}
}

func TestGetHost(t *testing.T) {
	host := getHost("test:*")

	if host != "*" {
		t.Errorf("getHost failed, expected %s, got %s", "*", host)
	}
}

func TestVersion(t *testing.T) {
	out, _ := captureOutput(func() error { return printVersion() })
	trimOut := strings.TrimSuffix(out, "\n")

	re := regexp.MustCompile(`KAFKA_OPS_VERSION\s*\??=\s*(.+)`)
	makefile, err := ioutil.ReadFile("Makefile")

	if err != nil {
		t.Fatal("Failed to read Makefile: " + err.Error())
	}
	v := re.FindAllSubmatch(makefile, -1)
	expected := string(v[0][1])
	if trimOut != expected {
		t.Fatalf("Version output %s does not match the expected %s", trimOut, expected)
	}
}
