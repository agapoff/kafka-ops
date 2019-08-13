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
    broker      string
    specfile    string
    protocol    string
    mechanism   string
    username    string
    password    string
    verbose     bool
    isYAML      bool
    isJSON      bool
    actionApply bool
    actionDump  bool
    errorStop   bool
)

type Spec struct {
    Topics []Topic                      `yaml:"topics" json:"topics"`
    Acls []Acl                          `yaml:"acls" json:"acls"`
}

type Topic struct {
    Name              string            `yaml:"name" json:"name"`
    Partitions        int               `yaml:"partitions" json:"partitions"`
    ReplicationFactor int               `yaml:"replication_factor" json:"replication_factor"`
    Configs           map[string]string `yaml:"configs" json:"configs"`
}

type Acl struct {
    Principal   string                  `yaml:"principal" json:"principal"`
    Permissions []Permission            `yaml:"permissions" json:"permissions"`
}

type Permission struct {
    Resource                            `yaml:"resource" json:"resource"`
    Allow []string                      `yaml:"allow_operations,omitempty,flow" json:"allow_operations,omitempty"`
    Deny  []string                      `yaml:"deny_operations,omitempty" json:"deny_operations,omitempty"`
}

type Resource struct {
    Type        string                  `yaml:"type" json:"type"`
    Pattern     string                  `yaml:"pattern" json:"pattern"`
    PatternType string                  `yaml:"patternType" json:"patternType"`
}

type OperationHost string

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
    flag.BoolVar(&actionApply, "apply", false, "Apply spec-file to the broker, create all entities that do not exist there; this is the default action")
    flag.BoolVar(&actionDump, "dump", false, "Dump broker entities in YAML (default) or JSON format to stdout or to a file if --spec option is defined")
    flag.BoolVar(&isYAML, "yaml", false, "Spec-file is in YAML format (will try to detect format if none of --yaml or --json is set)")
    flag.BoolVar(&isJSON, "json", false, "Spec-file is in JSON format (will try to detect format if none of --yaml or --json is set)")
    flag.BoolVar(&errorStop, "stop-on-error", false, "Exit on first occurred error")
    flag.BoolVar(&verbose, "verbose", false, "Verbose output")
    flag.Parse()

    if !actionApply && !actionDump {
        actionApply = true
    }
    if actionApply && actionDump {
        panic("Please define one of the actions: --dump, --apply")
    }
    if isJSON && isYAML {
        panic("Please define one of the formats: --json, --yaml")
    }
    if broker == "" {
        broker = loadEnvVar("KAFKA_BROKER")
        if broker == "" {
            broker = "localhost:9092"
        }
    }
    if specfile == "" {
        specfile = loadEnvVar("KAFKA_SPEC_FILE")
        if specfile == "" && actionApply {
            panic("Please define spec file with --spec option or with KAFKA_SPEC_FILE env variable")
        }
    }
    protocol = strings.ToLower(protocol)
    mechanism = strings.ToLower(mechanism)
    if protocol != "plaintext" {
        if username == "" {
            username = loadEnvVar("KAFKA_USERNAME")
        }
        if password == "" {
            password = loadEnvVar("KAFKA_PASSWORD")
        }
    }
}

func main() {
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
            config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &client.XDGSCRAMClient{HashGeneratorFcn: client.SHA256} }
        } else if mechanism == "scram-sha-512" {
            config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
            config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &client.XDGSCRAMClient{HashGeneratorFcn: client.SHA512} }
        } else {
            panic("The only supported SASL mechanisms: scram-sha-256, scram-sha-512")
        }
    }

    admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
    if err != nil {
        panic("Error while creating cluster admin: " + err.Error())
    }
    defer func() { _ = admin.Close() }()

    if actionApply {
        applySpecFile(&admin)
    } else if actionDump {
        dumpSpec(&admin)
    }
}

func dumpSpec(adminref *sarama.ClusterAdmin) {
    admin := *adminref

    // Get current topics from broker
    currentTopics, err := admin.ListTopics()
    if err != nil {
        panic(err)
    }

    /*
    r := sarama.Resource{ResourceType: sarama.AclResourceTopic, ResourceName: "Hello", ResourcePatternType: sarama.AclPatternLiteral}
    a := sarama.Acl{Principal: "User:test", Host: "*", Operation: sarama.AclOperationRead, PermissionType: sarama.AclPermissionAllow}
    err = admin.CreateACL(r, a)
    if err != nil {
        panic(err)
    }
    */

    /*
    filter := sarama.AclFilter{
        Version: 1,
        ResourceType: sarama.AclResourceAny,
        Operation:    sarama.AclOperationAny,
        ResourcePatternTypeFilter: sarama.AclPatternAny,
        PermissionType: sarama.AclPermissionAny,
    }

    currentAcls, err := admin.ListAcls(filter)
    if err != nil {
        panic(err)
    }
    */
    currentAcls := listAllAcls(&admin)

    var spec Spec

    for name, currentTopic := range currentTopics {
        if strings.HasPrefix(name, "__") {
            continue
        }
        var topic Topic
        topic.Name = name
        topic.Partitions = int(currentTopic.NumPartitions)
        topic.ReplicationFactor = int(currentTopic.ReplicationFactor)
        topic.Configs = make(map[string]string)
        for key, val := range currentTopic.ConfigEntries {
            topic.Configs[key] = *val
        }
        spec.Topics = append(spec.Topics, topic)
    }

    for _, resourceAcls := range currentAcls {
        for _, currentAcl := range resourceAcls.Acls {
            var acl Acl
            var permission Permission
            var resource Resource

            principal := currentAcl.Principal
            acl.Principal = principal

            resource.Pattern = resourceAcls.Resource.ResourceName
            resource.PatternType = aclResourcePatternTypeToString(resourceAcls.Resource.ResourcePatternType)
            resource.Type = aclResourceTypeToString(resourceAcls.Resource.ResourceType)

            permission.Resource = resource
            if currentAcl.PermissionType == sarama.AclPermissionAllow {
                permission.Allow = append(permission.Allow, aclOperationToString(currentAcl.Operation) + ":" + currentAcl.Host)
            } else {
                permission.Deny = append(permission.Deny, aclOperationToString(currentAcl.Operation) + ":" + currentAcl.Host)
            }

            acl.Permissions = append(acl.Permissions, permission)
            spec.AddAcl(acl)
        }
    }

    if isJSON {
        jsonTopic,_ := json.MarshalIndent(spec, "", "    ")
        fmt.Printf(string(jsonTopic))
    } else {
        yamlTopic,_ := yaml.Marshal(spec)
        fmt.Printf(string(yamlTopic))
    }
}

func (s *Spec) AddAcl(acl Acl) {
    for i, a := range s.Acls {
        if a.Principal == acl.Principal {
            for j, p := range a.Permissions {
                if p.Resource.Equals(acl.Permissions[0].Resource) {
                    s.Acls[i].Permissions[j].Allow = append(s.Acls[i].Permissions[j].Allow, acl.Permissions[0].Allow...)
                    s.Acls[i].Permissions[j].Deny = append(s.Acls[i].Permissions[j].Deny, acl.Permissions[0].Deny...)
                    return
                }
            }
            s.Acls[i].Permissions = append(s.Acls[i].Permissions, acl.Permissions...)
            return
        }
    }
    s.Acls = append(s.Acls, acl)
}

func (r Resource) Equals(res Resource) bool {
    return r.Type == res.Type && r.Pattern == res.Pattern && r.PatternType == res.PatternType
}

func (o OperationHost) Operation() sarama.AclOperation {
    //s := strings.Split(string(o), ":")
    return aclOperationFromString(o.OperationName())
}

func (o OperationHost) OperationName() string {
    return strings.Split(string(o), ":")[0]
}

func (o OperationHost) Host() string {
    return strings.Split(string(o), ":")[1]
}

func listAllAcls(adminref *sarama.ClusterAdmin) []sarama.ResourceAcls {
    filter := sarama.AclFilter{
        Version: 1,
        ResourceType: sarama.AclResourceAny,
        Operation:    sarama.AclOperationAny,
        ResourcePatternTypeFilter: sarama.AclPatternAny,
        PermissionType: sarama.AclPermissionAny,
    }

    currentAcls, err := (*adminref).ListAcls(filter)
    if err != nil {
        panic(err)
    }
    return currentAcls
}

func applySpecFile(adminref *sarama.ClusterAdmin) {
    admin := *adminref

    var numOk, numChanged, numError int

    spec := parseSpecFile()

    // Get number of brokers
    brokers, _, err := admin.DescribeCluster()
    if err != nil {
        panic(err)
    }
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

    // Iterate over topics
    for _, topic := range spec.Topics {
        if topic.ReplicationFactor < 1 {
            topic.ReplicationFactor = autoReplicationFactor
        }
        fmt.Printf("TASK [TOPIC : Create topic %s (partitions=%d, replicas=%d)] %s\n", topic.Name, topic.Partitions, topic.ReplicationFactor, strings.Repeat("*", 25))
        currentTopic, found := currentTopics[topic.Name]

        if !found {
            // Topic doesn't exist - need to create one
            err := createTopic(topic, &admin)
            if err != nil {
                printResult(Error, broker, err.Error(), topic)
                numError++
                if errorStop {
                    break
                } else {
                    continue
                }
            }
            printResult(Changed, broker, "", topic)
            numChanged++
        } else {
            // Topic exists
            var topicAltered bool = false
            var topicConfigAlterNeeded = false
            // Check the replication-factor
            if int16(topic.ReplicationFactor) != currentTopic.ReplicationFactor {
                printResult(Error, broker, "Cannot change replication-factor. Consider doing it manually with kafka-reassign-partitions utility or re-creating the topic", topic)
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
                    printResult(Error, broker, err.Error(), topic)
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
                        //fmt.Printf("Current %s: %#v New: %#v\n", key, *currentVal, val)
                        break
                    }
                } else if val != "default" {
                    topicConfigAlterNeeded = true
                    //fmt.Printf("New %s: %#v\n", key, val)
                    break
                }
            }
            if topicConfigAlterNeeded {
                topic,err = alterTopicConfig(topic, &admin, currentTopic.ConfigEntries)
                if err != nil {
                    printResult(Error, broker, err.Error(), topic)
                    numError++
                    if errorStop {
                        break
                    } else {
                        continue
                    }
                }
                topicAltered = true
            }

            if topicAltered {
                printResult(Changed, broker, "", topic)
                numChanged++
            } else {
                printResult(Ok, broker, "", topic)
                numOk++
            }
        }
    }

    // Get current ACLs from broker
    currentAcls := listAllAcls(&admin)

    // Iterate over ACLs
        /*
    r := sarama.Resource{ResourceType: sarama.AclResourceTopic, ResourceName: "Hello", ResourcePatternType: sarama.AclPatternLiteral}
    a := sarama.Acl{Principal: "User:test", Host: "*", Operation: sarama.AclOperationRead, PermissionType: sarama.AclPermissionAllow}
    err = admin.CreateACL(r, a)
    if err != nil {
        panic(err)
    }
    */
    for _, acl := range spec.Acls {
        principal := acl.Principal
        for _, permission := range acl.Permissions {
            resource := permission.Resource
            for _, allow := range permission.Allow {
                oh := OperationHost(allow)
                fmt.Printf("TASK [ACL : Create ACL (ALLOW %s@%s to %s %s:%s:%s)] %s\n",
                     principal, oh.Host(), oh.OperationName(), resource.Type, resource.PatternType, resource.Pattern, strings.Repeat("*", 25))
                if aclExists(&currentAcls, sarama.AclPermissionAllow, principal, oh.OperationName(), resource.Type, resource.PatternType, resource.Pattern, oh.Host()) {
                    printResult(Ok, broker, "", nil)
                    numOk++
                } else {
                    r := sarama.Resource{
                        ResourceType: aclResourceTypeFromString(resource.Type),
                        ResourceName: resource.Pattern,
                        ResourcePatternType: aclResourcePatternTypeFromString(resource.PatternType),
                    }
                    a := sarama.Acl{
                        Principal: principal,
                        Host: oh.Host(),
                        Operation: aclOperationFromString(oh.OperationName()),
                        PermissionType: sarama.AclPermissionAllow,
                    }
                    err = admin.CreateACL(r, a)
                    if err != nil {
                        printResult(Error, broker, err.Error(), acl)
                        numError++
                        if errorStop {
                            break
                            // TODO : break from all loops
                        } else {
                            continue
                        }
                    }
                    printResult(Changed, broker, "", acl)
                    numChanged++
                }
            }
        }
    }

    //for _, resourceAcls := range currentAcls {
    //    for _, currentAcl := range resourceAcls.Acls {

    printSummary(broker, numOk, numChanged, numError)
    if numError > 0 {
        os.Exit(2)
    }
}

func aclExists(a *[]sarama.ResourceAcls, p sarama.AclPermissionType, principal string, operation string, resource string, patternType string, pattern string, host string) bool {
    for _, resourceAcls := range *a {
        for _, currentAcl := range resourceAcls.Acls {
            //fmt.Printf("Compare %s=%s %s=%s %s=%s\n", principal, currentAcl.Principal, operation, aclOperationToString(currentAcl.Operation), resource, aclResourceTypeToString(resourceAcls.Resource.ResourceType) )
            //fmt.Printf("Compare %s=%s %s=%s %s=%s\n", patternType, aclResourcePatternTypeToString(resourceAcls.Resource.ResourcePatternType), pattern, resourceAcls.Resource.ResourceName, host, currentAcl.Host)
            if principal == currentAcl.Principal &&
               strings.ToUpper(operation) == aclOperationToString(currentAcl.Operation) &&
               strings.ToLower(resource) == aclResourceTypeToString(resourceAcls.Resource.ResourceType) &&
               strings.ToUpper(patternType) == aclResourcePatternTypeToString(resourceAcls.Resource.ResourcePatternType) &&
               pattern == resourceAcls.Resource.ResourceName &&
               host == currentAcl.Host &&
               p == currentAcl.PermissionType {
                  return true
               }
        }
    }
    return false
}

func parseSpecFile() Spec {
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
            err = json.Unmarshal(specFile, &spec)
            if err != nil {
                panic(err)
            }
        }
    }
    return spec
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
    return err
}

func printResult(color string, broker string, msg string, debug interface{}) {
    var status string
    switch color {
    case Ok:
        status = "ok"
    case Changed:
        status = "changed"
    case Error:
        status = "error"
    }
    jsonDebug := ""
    if verbose {
        json,_ := json.MarshalIndent(debug, "", "    ")
        jsonDebug = string(json)
    }
    fmt.Printf(color + status + ": [%s] %s\n%s\n" + Default, broker, msg, jsonDebug)
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

func aclOperationToString(operation sarama.AclOperation) string {
    switch operation {
    case sarama.AclOperationAny:
        return "ANY"
    case sarama.AclOperationAll:
        return "ALL"
    case sarama.AclOperationRead:
        return "READ"
    case sarama.AclOperationWrite:
        return "WRITE"
    case sarama.AclOperationCreate:
        return "CREATE"
    case sarama.AclOperationDelete:
        return "DELETE"
    case sarama.AclOperationAlter:
        return "ALTER"
    case sarama.AclOperationDescribe:
        return "DESCRIBE"
    case sarama.AclOperationClusterAction:
        return "CLUSTER_ACTION"
    case sarama.AclOperationDescribeConfigs:
        return "DESCRIBE_CONFIGS"
    case sarama.AclOperationAlterConfigs:
        return "ALTER_CONFIGS"
    case sarama.AclOperationIdempotentWrite:
        return "IDEMPOTENT_WRITE"
    default:
        return "INVALID"
    }
}

func aclOperationFromString(operation string) sarama.AclOperation {
    switch strings.ToUpper(operation) {
    case "ANY":
        return sarama.AclOperationAny
    case "ALL":
        return sarama.AclOperationAll
    case "READ":
        return sarama.AclOperationRead
    case "WRITE":
        return sarama.AclOperationWrite
    case "CREATE":
        return sarama.AclOperationCreate
    case "DELETE":
        return sarama.AclOperationDelete
    case "ALTER":
        return sarama.AclOperationAlter
    case "DESCRIBE":
        return sarama.AclOperationDescribe
    case "CLUSTER_ACTION":
        return sarama.AclOperationClusterAction
    case "DESCRIBE_CONFIGS":
        return sarama.AclOperationDescribeConfigs
    case "ALTER_CONFIGS":
        return sarama.AclOperationAlterConfigs
    case "IDEMPOTENT_WRITE":
        return sarama.AclOperationIdempotentWrite
    default:
        return sarama.AclOperationUnknown
    }
}

func aclResourcePatternTypeToString(patternType sarama.AclResourcePatternType) string {
    switch patternType {
    case sarama.AclPatternAny:
        return "ANY"
    case sarama.AclPatternMatch:
        return "MATCH"
    case sarama.AclPatternLiteral:
        return "LITERAL"
    case sarama.AclPatternPrefixed:
        return "PREFIXED"
    default:
        return "INVALID"
    }
}

func aclResourcePatternTypeFromString(patternType string) sarama.AclResourcePatternType {
    switch strings.ToUpper(patternType) {
    case "ANY":
        return sarama.AclPatternAny
    case "MATCH":
        return sarama.AclPatternMatch
    case "LITERAL":
        return sarama.AclPatternLiteral
    case "PREFIXED":
        return sarama.AclPatternPrefixed
    default:
        return sarama.AclPatternUnknown
    }
}

func aclResourceTypeToString(resourceType sarama.AclResourceType) string {
    switch resourceType {
    case sarama.AclResourceAny:
        return "any"
    case sarama.AclResourceTopic:
        return "topic"
    case sarama.AclResourceGroup:
        return "group"
    case sarama.AclResourceCluster:
        return "cluster"
    case sarama.AclResourceTransactionalID:
        return "transactional-id"
    default:
        return "INVALID"
    }
}

func aclResourceTypeFromString(resourceType string) sarama.AclResourceType {
    switch strings.ToLower(resourceType) {
    case "any":
        return sarama.AclResourceAny
    case "topic":
        return sarama.AclResourceTopic
    case "group":
        return sarama.AclResourceGroup
    case "cluster":
        return sarama.AclResourceCluster
    case "transactional-id":
        return sarama.AclResourceTransactionalID
    default:
        return  sarama.AclResourceUnknown
    }
}

func usage() {
    fmt.Fprintf(os.Stderr, "Usage: %s -b <bootstrap brokers> -s <specfile> [-v]\n", os.Args[0])
    flag.PrintDefaults()
    os.Exit(1)
}