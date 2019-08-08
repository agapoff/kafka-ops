package main

import (
    "github.com/Shopify/sarama"
    "github.com/agapoff/kafka-ops/client"

    "strings"
    "fmt"
    "os"
    "flag"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "encoding/json"
)

var (
    broker    string
    specfile  string
    protocol  string
    mechanism string
    username  string
    password  string
    verbose   bool
    isYAML    bool
    isJSON    bool
    errorStop bool
)

type Spec struct {
    Topics []Topic `yaml:"topics"`
}

type Topic struct {
    Name              string            `yaml:"name" json:"name"`
    Partitions        int               `yaml:"partitions" json:"partitions"`
    ReplicationFactor int               `yaml:"replication_factor" json:"replication_factor"`
    Configs           map[string]string `yaml:"configs" json:"configs"`
}

const (
    Ok      = "\033[0;32m"
    Changed = "\033[0;33m"
    Error   = "\033[0;31m"
    Default = "\033[0m"
)

func init() {
    flag.StringVar(&broker, "broker", "", "Bootstrap-brokers, default is localhost:9092 (can be also set by Env variable KAFKA_BROKER)")
    flag.StringVar(&specfile, "spec", "", "Spec-file (can be set by Env variable KAFKA_SPEC_FILE)")
    flag.StringVar(&protocol, "protocol", "plaintext", "Security protocol. Available options: plaintext, sasl_ssl, sasl_plaintext (default: plaintext)")
    flag.StringVar(&mechanism, "mechanism", "scram-sha-256", "SASL mechanism. Available options: scram-sha-256, scram-sha-512 (default: scram-sha-256)")
    flag.StringVar(&username, "username", "", "Username for authentication (can be also set by Env variable KAFKA_USERNAME")
    flag.StringVar(&password, "password", "", "Password for authentication (can be also set by Env variable KAFKA_PASSWORD")
    flag.BoolVar(&isYAML, "yaml", false, "Spec-file is in YAML format (will try to detect format if none of --yaml or --json is set)")
    flag.BoolVar(&isJSON, "json", false, "Spec-file is in JSON format (will try to detect format if none of --yaml or --json is set)")
    flag.BoolVar(&errorStop, "stop-on-error", false, "Exit on first occurred error")
    flag.BoolVar(&verbose, "verbose", false, "Verbose output")
    flag.Parse()
    if broker == "" {
        broker = loadEnvVar("KAFKA_BROKER")
        if broker == "" {
            broker = "localhost:9092"
        }
    }
    if specfile == "" {
        specfile = loadEnvVar("KAFKA_SPEC_FILE")
        if specfile == "" {
            usage()
        }
    }
    protocol = strings.ToLower(protocol)
    mechanism = strings.ToLower(mechanism)
    if protocol != "plaintext" {
        if username == "" {
            username = loadEnvVar("KAFKA_USERNAME")
        }
        if password == "" {
            username = loadEnvVar("KAFKA_PASSWORD")
        }
    }
}

func main() {
    specFile, err := ioutil.ReadFile(specfile)
    if err != nil {
        panic(err)
    }

    var spec Spec
    if (isYAML) {
        err = yaml.Unmarshal(specFile, &spec)
        if err != nil {
            panic(err)
        }
    } else if (isJSON) {
        err = json.Unmarshal(specFile, &spec)
        if err != nil {
            panic(err)
        }
    } else {
        err = yaml.Unmarshal(specFile, &spec)
        if err != nil {
            err2 := json.Unmarshal(specFile, &spec)
            if err2 != nil {
                panic(err2)
            }
        }
    }

    // Connect to Kafka broker
    brokerAddrs := strings.Split(broker, ",")
    config := sarama.NewConfig()
    config.Version = sarama.V2_2_0_0

    if strings.HasPrefix(protocol, "sasl_") {
        config.Net.SASL.Enable = true
        config.Net.SASL.User = username
        config.Net.SASL.Password = password
        if mechanism == "scram-sha-256" {
            config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
            config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
        } else if mechanism == "scram-sha-512" {
            config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
            config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
        } else {
            panic("The only supported SASL mechanisms: scram-sha-256, scram-sha-512")
        }
    }

    admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
    if err != nil {
        panic("Error while creating cluster admin: "+err.Error())
    }
    defer func() { _ = admin.Close() }()

    // Get brokers
    brokers, _, _ := admin.DescribeCluster()
    var autoReplicationFactor int
    if len(brokers) > 1 {
        autoReplicationFactor = 2
    } else {
        autoReplicationFactor = 1
    }

    // Get current topics from broker
    currentTopics, err := admin.ListTopics()
    if err != nil {
        panic(err)
    }
    //fmt.Printf("\nDescr\n-- %#v --\n",currentTopics["my-topic1"])

    var numOk int = 0
    var numChanged int = 0
    var numError int = 0

    // Iterate over topics
    for _, topic := range spec.Topics {
        if topic.ReplicationFactor < 1 {
            topic.ReplicationFactor = autoReplicationFactor
        }
        fmt.Printf("TASK [TOPIC : Create topic %s (partitions=%d, replicas=%d)] %s\n", topic.Name, topic.Partitions, topic.ReplicationFactor, strings.Repeat("*", 25))
        currentTopic, found := currentTopics[topic.Name]

        if !found {
            // Topic doesn't exist - need to create one
            createTopic(topic, &admin)
            numChanged++
        } else {
            // Topic exists
            var topicAltered bool = false
            var topicConfigAlterNeeded = false
            jsonTopic,_ := json.MarshalIndent(topic, "", "    ")
            // Check the replication-factor
            if int16(topic.ReplicationFactor) != currentTopic.ReplicationFactor {
                printResult(Error, broker, "Cannot change replication-factor. Consider doing it manually with kafka-reassign-partitions utility or re-creating the topic", jsonTopic)
                numError++
                if errorStop {
                    break
                } else {
                    continue
                }
            }

            // Check the partitions count 
            //fmt.Printf("\nPartitions: %+v\n", currentTopic)
            if int32(topic.Partitions) != currentTopic.NumPartitions {
                //fmt.Printf("\nPartitions: %v, need %v\n", currentTopic.NumPartitions, topic.Partitions)
                err := alterNumPartitions(topic.Name, &admin, topic.Partitions)
                if err != nil {
                    printResult(Error, broker, err.Error(), jsonTopic)
                    numError++
                    if errorStop {
                        break
                    } else {
                        continue
                    }
                }
                topicAltered = true
            }
            // Check the configs
            for key, val := range topic.Configs {
                currentVal, found := currentTopic.ConfigEntries[key]
                if found {
                    if val != *currentVal {
                        topicConfigAlterNeeded = true
                        //fmt.Printf("Current: %#v New: %#v\n", *currentVal, val)
                        break
                    }
                } else if val != "default" {
                    topicConfigAlterNeeded = true
                    //fmt.Printf("New: %#v\n", val)
                    break
                }
            }
            if topicConfigAlterNeeded {
                topic,err = alterTopicConfig(topic, &admin, currentTopic.ConfigEntries)
                jsonTopic,_ = json.MarshalIndent(topic, "", "    ")
                topicAltered = true
            }

            if topicAltered {
                printResult(Changed, broker, "", jsonTopic)
                numChanged++
            } else {
                printResult(Ok, broker, "", jsonTopic)
                numOk++
            }
        }


    }

    printSummary(broker, numOk, numChanged, numError)
    if numError > 0 {
        os.Exit(2)
    }
}

func alterNumPartitions(topic string, clusterAdmin *sarama.ClusterAdmin, count int) error {
    admin := *clusterAdmin
    err := admin.CreatePartitions(topic, int32(count), nil, false)
    return err
}

func alterTopicConfig(topic Topic, clusterAdmin *sarama.ClusterAdmin, currentConfig map[string]*string) (Topic,error) {
    admin := *clusterAdmin
    configEntries := make(map[string]*string)
    for key, val := range topic.Configs {
        if val != "default" {
            configEntries[key] = getPtr(topic.Configs[key])
        }
    }
    for key, val := range currentConfig {
        if _, found := topic.Configs[key]; !found {
            configEntries[key] = val
            topic.Configs[key] = *val
        }
    }
    err := admin.AlterConfig(sarama.TopicResource, topic.Name, configEntries, false)
    if err != nil {
        panic("Error while creating topic: "+err.Error())
    }
    return topic, err
}

func createTopic(topic Topic, clusterAdmin *sarama.ClusterAdmin) error {
    admin := *clusterAdmin
    configEntries := make(map[string]*string)
    for key, val := range topic.Configs {
        if val != "default" {
            configEntries[key] = getPtr(topic.Configs[key])
        }
    }
    err := admin.CreateTopic(topic.Name, &sarama.TopicDetail{
        NumPartitions:     int32(topic.Partitions),
        ReplicationFactor: int16(topic.ReplicationFactor),
        ConfigEntries:     configEntries,
    }, false)
    if err != nil {
        panic("Error while creating topic: "+err.Error())
    }
    jsonTopic,_ := json.MarshalIndent(topic, "", "    ")
    printResult(Changed, broker, "", jsonTopic)
    return err
}

func printResult(color string, broker string, msg string, debug []byte) {
    var status string
    switch color {
    case Ok:
        status = "ok"
    case Changed:
        status = "changed"
    case Error:
        status = "error"
    }
    if !verbose {
        debug = nil
    }
    fmt.Printf(color + status + ": [%s] %s\n%s\n" + Default, broker, msg, string(debug))
}

func printSummary(broker string, numOk int, numChanged int, numError int) {
    fmt.Printf("SUMMARY %s\n", strings.Repeat("*", 80))
    if numOk > 0 {
        fmt.Printf(Ok)
    }
    fmt.Printf(" ok=%d   " + Default, numOk)
    if numChanged > 0 {
        fmt.Printf(Changed)
    }
    fmt.Printf(" changed=%d   " + Default, numChanged)
    if numError > 0 {
        fmt.Printf(Error)
    }
    fmt.Printf(" failed=%d\n" + Default, numError)
}

func loadEnvVar(key string) string {
    if val, ok := os.LookupEnv(key); ok {
        return val
    }
    return ""
}

func getPtr(s string) *string {
    return &s
}

func usage() {
    fmt.Fprintf(os.Stderr, "Usage: %s -b <bootstrap brokers> -s <specfile> [-v]\n", os.Args[0])
    flag.PrintDefaults()
    os.Exit(1)
}
