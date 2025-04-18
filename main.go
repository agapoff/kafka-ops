package main

import (
	"github.com/IBM/sarama"

	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"text/template"
)

const version string = "1.0.5"

var (
	broker        string
	specfile      string
	protocol      string
	mechanism     string
	username      string
	password      string
	verbose       bool
	isYAML        bool
	isJSON        bool
	actionApply   bool
	actionDump    bool
	actionHelp    bool
	actionVersion bool
	errorStop     bool
	isTemplate    bool
	missingOk     bool
	varFlags      arrFlags
)

type arrFlags []string

// Spec contains the full structure of the manifest
type Spec struct {
	Topics         []Topic         `yaml:"topics" json:"topics"`
	Acls           []Acl           `yaml:"acls" json:"acls"`
	ConsumerGroups []ConsumerGroup `yaml:"consumer-groups,omitempty" json:"consumer-groups,omitempty"`
	Connection     Connection      `yaml:"connection,omitempty" json:"connection,omitempty"`
}

// Topic describes single topic
type Topic struct {
	Name              string            `yaml:"name" json:"name"`
	Partitions        int               `yaml:"partitions" json:"partitions"`
	ReplicationFactor int               `yaml:"replication_factor" json:"replication_factor"`
	Configs           map[string]string `yaml:"configs" json:"configs"`
	State             string            `yaml:"state,omitempty" json:"state,omitempty"`
	PatternType       string            `yaml:"patternType,omitempty" json:"patternType,omitempty"`
	Matched           []string          `yaml:"matched,omitempty" json:"matched,omitempty"`
}

// ConsumerGroup describes a consumer group to be deleted
type ConsumerGroup struct {
	Name        string   `yaml:"name" json:"name"`
	State       string   `yaml:"state,omitempty" json:"state,omitempty"`
	PatternType string   `yaml:"patternType,omitempty" json:"patternType,omitempty"`
	Matched     []string `yaml:"matched,omitempty" json:"matched,omitempty"`
}

// Acl describes single ACL
type Acl struct {
	Principal   string       `yaml:"principal" json:"principal"`
	Permissions []Permission `yaml:"permissions" json:"permissions"`
}

// Permission contains all permissions for a single resource (topic, group, cluster)
type Permission struct {
	Resource Resource `yaml:"resource" json:"resource"`
	Allow    []string `yaml:"allow_operations,omitempty,flow" json:"allow_operations,omitempty"`
	Deny     []string `yaml:"deny_operations,omitempty" json:"deny_operations,omitempty"`
	State    string   `yaml:"state,omitempty" json:"state,omitempty"`
}

// Resource contains the description of the resource (topic, group, cluster)
type Resource struct {
	Type        string `yaml:"type" json:"type"`
	Pattern     string `yaml:"pattern" json:"pattern"`
	PatternType string `yaml:"patternType" json:"patternType"`
}

// SingleACL contains one permission for a single resource
type SingleACL struct {
	PermissionType string   `json:"permission_type"`
	Principal      string   `json:"principal"`
	Resource       Resource `json:"resource"`
	Operation      string   `json:"operation"`
	Host           string   `json:"host,omitempty"`
	State          string   `json:"state"`
}

// Connection describes the brokers settings defined in the manifest
type Connection struct {
	Broker    string `yaml:"broker,omitempty" json:"broker,omitempty"`
	Protocol  string `yaml:"protocol,omitempty" json:"protocol,omitempty"`
	Mechanism string `yaml:"mechanism,omitempty" json:"mechanism,omitempty"`
	Username  string `yaml:"username,omitempty" json:"username,omitempty"`
	Password  string `yaml:"password,omitempty" json:"password,omitempty"`
}

// Exit is used for handling panics
type Exit struct{ Code int }

// The values for coloring the output
const (
	Ok      = "\033[0;32m"
	Changed = "\033[0;33m"
	Error   = "\033[0;31m"
	Default = "\033[0m"
)

func main() {
	defer handleExit()
	//defer fmt.Println("closed")
	validateFlags()

	if actionApply {
		err := applySpecFile()
		if err != nil {
			if err.Error() != "" {
				fmt.Println(err.Error())
			}
			panic(Exit{2})
		}
	} else if actionDump {
		err := dumpSpec()
		if err != nil {
			if err.Error() != "" {
				fmt.Println(err.Error())
			}
			panic(Exit{2})
		}
	} else if actionHelp {
		usage()
	} else if actionVersion {
		printVersion()
	}
}

func connectToKafkaCluster() (*sarama.ClusterAdmin, error) {
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
			return nil, errors.New("The only supported SASL mechanisms: scram-sha-256, scram-sha-512")
		}
	}
	if strings.HasSuffix(protocol, "_ssl") {
		config.Net.TLS.Enable = true
		tlsConfig := tls.Config{
			InsecureSkipVerify: true,
		}
		config.Net.TLS.Config = &tlsConfig
	}

	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
	if err != nil {
		return nil, errors.New("Error while creating cluster admin: " + err.Error())
	}
	return &admin, nil
}

func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(Exit); ok == true {
			os.Exit(exit.Code)
		}
		panic(e)
	}
}

func dumpSpec() error {
	admin, err := connectToKafkaCluster()
	if err != nil {
		return err
	}
	defer func() { _ = (*admin).Close() }()
	// Get current topics from broker
	currentTopics, err := (*admin).ListTopics()
	if err != nil {
		return err
	}

	// Get current ACLs from broker
	currentAcls, err := listAllAcls(admin)
	if err != nil {
		return err
	}

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
				permission.Allow = append(permission.Allow, aclOperationToString(currentAcl.Operation)+":"+currentAcl.Host)
			} else {
				permission.Deny = append(permission.Deny, aclOperationToString(currentAcl.Operation)+":"+currentAcl.Host)
			}

			acl.Permissions = append(acl.Permissions, permission)
			spec.AddAcl(acl)
		}
	}

	if isJSON {
		jsonTopic, _ := json.MarshalIndent(spec, "", "    ")
		fmt.Printf(string(jsonTopic))
	} else {
		yamlTopic, _ := yaml.Marshal(spec)
		fmt.Printf(string(yamlTopic))
	}
	return nil
}

// AddAcl combines permissions with common Resource
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

// Equals compares Resource structs
func (r Resource) Equals(res Resource) bool {
	return r.Type == res.Type && r.Pattern == res.Pattern && r.PatternType == res.PatternType
}

func getHost(s string) string {
	split := strings.Split(string(s), ":")
	if len(split) > 1 {
		return split[1]
	}
	return ""
}

func getOperation(s string) string {
	return strings.Split(string(s), ":")[0]
}

func listAllAcls(admin *sarama.ClusterAdmin) ([]sarama.ResourceAcls, error) {
	filter := sarama.AclFilter{
		ResourceType:              sarama.AclResourceAny,
		Operation:                 sarama.AclOperationAny,
		ResourcePatternTypeFilter: sarama.AclPatternAny,
		PermissionType:            sarama.AclPermissionAny,
	}

	currentAcls, err := (*admin).ListAcls(filter)
	if err != nil {
		return nil, err
	}
	return currentAcls, nil
}

func applySpecFile() error {
	var numOk, numChanged, numError int

	spec, err := parseSpecFile()
	if err != nil {
		return errors.New("Can't parse spec manifest: " + err.Error())
	}

	if spec.Connection.Broker != "" {
		broker = spec.Connection.Broker
	}
	if spec.Connection.Protocol != "" {
		protocol = strings.ToLower(spec.Connection.Protocol)
	}
	if spec.Connection.Mechanism != "" {
		mechanism = strings.ToLower(spec.Connection.Mechanism)
	}
	if spec.Connection.Username != "" {
		username = spec.Connection.Username
	}
	if spec.Connection.Password != "" {
		password = spec.Connection.Password
	}
	if broker == "" {
		broker = "localhost:9092"
	}

	admin, err := connectToKafkaCluster()
	if err != nil {
		return err
	}
	defer func() { _ = (*admin).Close() }()

	// Get number of brokers
	brokers, _, err := (*admin).DescribeCluster()
	if err != nil {
		return errors.New("Can't get number of brokers: " + err.Error())
	}
	var autoReplicationFactor int
	if len(brokers) > 1 {
		autoReplicationFactor = 2
	} else {
		autoReplicationFactor = 1
	}

	// Get current topics from broker
	currentTopics, err := (*admin).ListTopics()
	if err != nil {
		return errors.New("Can't list topics: " + err.Error())
	}

	// Iterate over topics
	for _, topic := range spec.Topics {

		if topic.State == "absent" {
			topic.PatternType = strings.ToLower(topic.PatternType)
			if topic.PatternType == "prefixed" || topic.PatternType == "match" {
				// Delete topics by pattern
				fmt.Printf("TASK [TOPIC : Delete topics %s by %s] %s\n", topic.PatternType, topic.Name, strings.Repeat("*", 42))
				var currentState = Ok
				var currentError = ""
				for currentTopicName, _ := range currentTopics {
					var matched = false
					if topic.PatternType == "prefixed" {
						matched = strings.HasPrefix(currentTopicName, topic.Name)
					} else if topic.PatternType == "match" {
						matched, _ = regexp.MatchString(topic.Name, currentTopicName)
					}
					if matched {
						topic.Matched = append(topic.Matched, currentTopicName)
						err := deleteTopic(currentTopicName, admin)
						if err != nil {
							numError++
							currentState = Error
							currentError = err.Error()
							if errorStop {
								printResult(Error, broker, err.Error(), topic)
								break
							}
						} else {
							numChanged++
							if currentState != Error {
								currentState = Changed
							}
						}
					}
				}
				printResult(currentState, broker, currentError, topic)
				if currentState == Ok {
					numOk++
				}
				continue
			} else {
				fmt.Printf("TASK [TOPIC : Delete topic %s] %s\n", topic.Name, strings.Repeat("*", 52))
			}
		} else {
			topic.State = "present"
		}

		currentTopic, found := currentTopics[topic.Name]

		if !found {
			// Topic doesn't exist - need to create one or no need to delete
			if topic.State == "absent" {
				printResult(Ok, broker, "", topic)
				numOk++
			} else {
				if topic.ReplicationFactor < 1 {
					topic.ReplicationFactor = autoReplicationFactor
				}
				fmt.Printf("TASK [TOPIC : Create topic %s (partitions=%d, replicas=%d)] %s\n", topic.Name, topic.Partitions, topic.ReplicationFactor, strings.Repeat("*", 25))
				err := createTopic(topic, admin)
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
			}
		} else {
			// Topic exists
			if topic.State == "absent" {
				err := deleteTopic(topic.Name, admin)
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
				var topicAltered bool = false
				var topicConfigAlterNeeded = false
				// Check the replication-factor
				if topic.ReplicationFactor > 0 {
					fmt.Printf("TASK [TOPIC : Modify topic %s (partitions=%d, replicas=%d)] %s\n", topic.Name, topic.Partitions, topic.ReplicationFactor, strings.Repeat("*", 25))
					if int16(topic.ReplicationFactor) != currentTopic.ReplicationFactor {
						printResult(Error, broker, "Cannot change replication-factor. Consider doing it manually with kafka-reassign-partitions utility or re-creating the topic", topic)
						numError++
						if errorStop {
							break
						} else {
							continue
						}
					}
				} else {
					fmt.Printf("TASK [TOPIC : Modify topic %s (partitions=%d)] %s\n", topic.Name, topic.Partitions, strings.Repeat("*", 37))
				}
				// Check the partitions count
				if int32(topic.Partitions) != currentTopic.NumPartitions {
					err := alterNumPartitions(topic.Name, admin, topic.Partitions)
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
							break
						}
					} else if val != "default" {
						topicConfigAlterNeeded = true
						break
					}
				}
				if topicConfigAlterNeeded {
					topic, err = alterTopicConfig(topic, admin, currentTopic.ConfigEntries)
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
	}

	if len(spec.ConsumerGroups) > 0 {
		// Get current consumer-groups from broker
		currentGroups, err := (*admin).ListConsumerGroups()
		if err != nil {
			return errors.New("Can't list consumer-groups: " + err.Error())
		}

		// Iterate over consumer-groups
		for _, group := range spec.ConsumerGroups {
			if group.State != "absent" {
				return errors.New("Consumer-groups support only state=absent")
			}
			group.PatternType = strings.ToLower(group.PatternType)
			if group.PatternType != "prefixed" && group.PatternType != "match" {
				group.PatternType = "literal"
			}

			// Delete consumer-groups by pattern
			fmt.Printf("TASK [CONSUMER-GROUP : Delete consumer-group %s by %s] %s\n", group.PatternType, group.Name, strings.Repeat("*", 25))
			var currentState = Ok
			var currentError = ""
			for currentGroupName, _ := range currentGroups {
				var matched = false
				if group.PatternType == "prefixed" {
					matched = strings.HasPrefix(currentGroupName, group.Name)
				} else if group.PatternType == "match" {
					matched, _ = regexp.MatchString(group.Name, currentGroupName)
				} else if currentGroupName == group.Name {
					matched = true
				}

				if matched {
					group.Matched = append(group.Matched, currentGroupName)
					err := DeleteConsumerGroup(currentGroupName, admin)
					if err != nil {
						numError++
						currentState = Error
						currentError = err.Error()
						if errorStop {
							printResult(Error, broker, err.Error(), group)
							break
						}
					} else {
						numChanged++
						if currentState != Error {
							currentState = Changed
						}
					}
				}
			}
			printResult(currentState, broker, currentError, group)
			if currentState == Ok {
				numOk++
			}
		}
	}

	if len(spec.Acls) > 0 {

		// Get current ACLs from broker
		currentAcls, err := listAllAcls(admin)
		if err != nil {
			return err
		}

		// Iterate over ACLs
		breakLoop := false
		for _, acl := range spec.Acls {
			principal := acl.Principal
			for _, permission := range acl.Permissions {
				resource := permission.Resource
				for i, rule := range append(permission.Allow, permission.Deny...) {
					sacl := SingleACL{
						Principal: principal,
						Resource:  resource,
						Operation: getOperation(rule),
						Host:      getHost(rule),
					}
					if permission.State == "absent" {
						sacl.State = "absent"
					} else {
						sacl.State = "present"
						// Host can be unset, we'll treat this as * for creating
						if sacl.Host == "" {
							sacl.Host = "*"
						}
					}
					if i < len(permission.Allow) {
						sacl.PermissionType = "ALLOW"
					} else {
						sacl.PermissionType = "DENY"
					}

					result, err := alignAcl(admin, &currentAcls, sacl)
					if result == Ok {
						printResult(Ok, broker, "", sacl)
						numOk++
					} else if err != nil {
						printResult(Error, broker, err.Error(), sacl)
						numError++
						if errorStop {
							breakLoop = true
							break
						}
					} else {
						printResult(result, broker, "", sacl)
						numChanged++
					}
				}
				if breakLoop {
					break
				}
			}
			if breakLoop {
				break
			}

		}
	}
	printSummary(broker, numOk, numChanged, numError)
	if numError > 0 {
		return errors.New("")
	}
	return nil
}

func alignAcl(admin *sarama.ClusterAdmin, acls *[]sarama.ResourceAcls, acl SingleACL) (string, error) {
	var action string
	if acl.State == "present" {
		action = "Create"
	} else {
		action = "Remove"
	}

	if acl.Resource.Type == "cluster" {
		if acl.Resource.Pattern == "" {
			acl.Resource.Pattern = "kafka-cluster"
		}
	}
	if acl.Resource.PatternType == "" {
		acl.Resource.PatternType = "LITERAL"
	}

	fmt.Printf("TASK [ACL : %s ACL (%s %s@%s to %s %s:%s:%s)] %s\n", action, acl.PermissionType, acl.Principal,
		acl.Host, acl.Operation, acl.Resource.Type, acl.Resource.PatternType, acl.Resource.Pattern, strings.Repeat("*", 25))

	if acl.Principal == "" {
		return Error, errors.New("Principal not defined")
	}

	if aclResourceTypeFromString(acl.Resource.Type) == sarama.AclResourceUnknown {
		return Error, errors.New("Wrong resource type: " + acl.Resource.Type)
	}

	if acl.State == "absent" {
		// Won't check the presence. We'll just try do delete and see the length of MatchingAcl in response
		filter := sarama.AclFilter{
			ResourceType:              aclResourceTypeFromString(acl.Resource.Type),
			ResourceName:              &acl.Resource.Pattern,
			ResourcePatternTypeFilter: aclResourcePatternTypeFromString(acl.Resource.PatternType),
			Operation:                 aclOperationFromString(acl.Operation),
			PermissionType:            aclPermissionTypeFromString(acl.PermissionType),
		}
		if acl.Host != "" {
			filter.Host = &acl.Host
		}
		if acl.Principal != "*" {
			filter.Principal = &acl.Principal
		}
		mAcls, err := (*admin).DeleteACL(filter, false)
		if err != nil {
			return Error, err
		}
		if len(mAcls) > 0 {
			return Changed, nil
		}
		return Ok, nil
	}

	if aclExists(admin, acls, acl) {
		return Ok, nil
	}

	r := sarama.Resource{
		ResourceType:        aclResourceTypeFromString(acl.Resource.Type),
		ResourceName:        acl.Resource.Pattern,
		ResourcePatternType: aclResourcePatternTypeFromString(acl.Resource.PatternType),
	}
	a := sarama.Acl{
		Principal:      acl.Principal,
		Host:           acl.Host,
		Operation:      aclOperationFromString(acl.Operation),
		PermissionType: aclPermissionTypeFromString(acl.PermissionType),
	}
	err := (*admin).CreateACL(r, a)
	return Changed, err
}

func aclExists(admin *sarama.ClusterAdmin, acls *[]sarama.ResourceAcls, acl SingleACL) bool {
	for _, resourceAcls := range *acls {
		for _, currentAcl := range resourceAcls.Acls {
			if acl.Principal == currentAcl.Principal &&
				strings.ToUpper(acl.Operation) == aclOperationToString(currentAcl.Operation) &&
				strings.ToLower(acl.Resource.Type) == aclResourceTypeToString(resourceAcls.Resource.ResourceType) &&
				strings.ToUpper(acl.Resource.PatternType) == aclResourcePatternTypeToString(resourceAcls.Resource.ResourcePatternType) &&
				acl.Resource.Pattern == resourceAcls.Resource.ResourceName &&
				acl.Host == currentAcl.Host &&
				acl.PermissionType == aclPermissionTypeToString(currentAcl.PermissionType) {
				return true
			}
		}
	}
	return false
}

func parseSpecFile() (Spec, error) {
	var spec Spec
	specFile, err := ioutil.ReadFile(specfile)
	if err != nil {
		return spec, err
	}

	if isTemplate {
		t := template.New("")
		if missingOk {
			t, err = t.Parse(string(specFile))
		} else {
			t, err = t.Option("missingkey=error").Parse(string(specFile))
		}
		if err != nil {
			return spec, err
		}
		config := loadEnvMap()
		var tpl bytes.Buffer
		err = t.Execute(&tpl, config)
		if err != nil {
			return spec, err
		}
		specFile = tpl.Bytes()
	}

	if isYAML {
		err = yaml.Unmarshal(specFile, &spec)
	} else if isJSON {
		err = json.Unmarshal(specFile, &spec)
	} else {
		err = yaml.Unmarshal(specFile, &spec)
		if err != nil {
			err = json.Unmarshal(specFile, &spec)
		}
	}
	return spec, err
}

func alterNumPartitions(topic string, clusterAdmin *sarama.ClusterAdmin, count int) error {
	admin := *clusterAdmin
	err := admin.CreatePartitions(topic, int32(count), nil, false)
	return err
}

func alterTopicConfig(topic Topic, clusterAdmin *sarama.ClusterAdmin, currentConfig map[string]*string) (Topic, error) {
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

func createTopic(topic Topic, admin *sarama.ClusterAdmin) error {
	configEntries := make(map[string]*string)
	for key, val := range topic.Configs {
		if val != "default" {
			configEntries[key] = getPtr(topic.Configs[key])
		}
	}
	err := (*admin).CreateTopic(topic.Name, &sarama.TopicDetail{
		NumPartitions:     int32(topic.Partitions),
		ReplicationFactor: int16(topic.ReplicationFactor),
		ConfigEntries:     configEntries,
	}, false)
	return err
}

func deleteTopic(topic string, admin *sarama.ClusterAdmin) error {
	err := (*admin).DeleteTopic(topic)
	return err
}

func DeleteConsumerGroup(group string, admin *sarama.ClusterAdmin) error {
	err := (*admin).DeleteConsumerGroup(group)
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
		json, _ := json.MarshalIndent(debug, "", "    ")
		jsonDebug = string(json)
	}
	fmt.Printf(color+status+": [%s] %s\n%s\n"+Default, broker, msg, jsonDebug)
}

func printSummary(broker string, numOk int, numChanged int, numError int) {
	fmt.Printf("SUMMARY %s\n", strings.Repeat("*", 80))
	if numOk > 0 {
		fmt.Printf(Ok)
	}
	fmt.Printf(" ok=%d   "+Default, numOk)
	if numChanged > 0 {
		fmt.Printf(Changed)
	}
	fmt.Printf(" changed=%d   "+Default, numChanged)
	if numError > 0 {
		fmt.Printf(Error)
	}
	fmt.Printf(" failed=%d\n"+Default, numError)
}

func loadEnvVar(key string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return ""
}

func loadEnvMap() map[string]string {
	items := make(map[string]string)
	for _, item := range append(os.Environ(), varFlags...) {
		splits := strings.Split(item, "=")
		key := splits[0]
		val := strings.Join(splits[1:], "=")
		items[key] = val
	}
	return items
}

func getPtr(s string) *string {
	return &s
}

func (f *arrFlags) Set(s string) error {
	*f = append(*f, s)
	return nil
}

func (f *arrFlags) String() string {
	return "-"
}

func validateFlags() {
	flag.StringVar(&broker, "broker", "", "Bootstrap-brokers, default is localhost:9092 (can be also set by Env variable KAFKA_BROKER)")
	flag.StringVar(&specfile, "spec", "", "Spec-file (can be set by Env variable KAFKA_SPEC_FILE)")
	flag.StringVar(&protocol, "protocol", "plaintext", "Security protocol. Available options: plaintext, sasl_ssl, sasl_plaintext (default: plaintext)")
	flag.StringVar(&mechanism, "mechanism", "scram-sha-256", "SASL mechanism. Available options: scram-sha-256, scram-sha-512 (default: scram-sha-256)")
	flag.StringVar(&username, "username", "", "Username for authentication (can be also set by Env variable KAFKA_USERNAME")
	flag.StringVar(&password, "password", "", "Password for authentication (can be also set by Env variable KAFKA_PASSWORD")
	flag.BoolVar(&actionApply, "apply", false, "Apply spec-file to the broker, create all entities that do not exist there; this is the default action")
	flag.BoolVar(&actionDump, "dump", false, "Dump broker entities in YAML (default) or JSON format to stdout or to a file if --spec option is defined")
	flag.BoolVar(&actionHelp, "help", false, "Print usage")
	flag.BoolVar(&actionVersion, "version", false, "Show version")
	flag.BoolVar(&isYAML, "yaml", false, "Spec-file is in YAML format (will try to detect format if none of --yaml or --json is set)")
	flag.BoolVar(&isJSON, "json", false, "Spec-file is in JSON format (will try to detect format if none of --yaml or --json is set)")
	flag.BoolVar(&errorStop, "stop-on-error", false, "Exit on first occurred error")
	flag.BoolVar(&isTemplate, "template", false, "Spec-file is a template")
	flag.BoolVar(&missingOk, "missingok", false, "Ignore missing template keys")
	flag.BoolVar(&verbose, "verbose", false, "Verbose output")
	flag.Var(&varFlags, "var", "Variable for templating")
	flag.Usage = func() {
		usage()
	}
	flag.Parse()

	protocol = strings.ToLower(protocol)
	mechanism = strings.ToLower(mechanism)

	if !actionApply && !actionDump && !actionHelp && !actionVersion {
		fmt.Println("Please define one of the actions: --dump, --apply, --help, --version")
		os.Exit(1)
	}
	if actionApply && actionDump {
		fmt.Println("Please define one of the actions: --dump, --apply. Refer to kafka-ops --help for details")
		os.Exit(1)
	}
	if isJSON && isYAML {
		fmt.Println("Please define one of the formats: --json, --yaml")
		os.Exit(1)
	}
	if broker == "" {
		broker = loadEnvVar("KAFKA_BROKER")
		if broker == "" && actionDump {
			broker = "localhost:9092"
		}
	}
	if specfile == "" {
		specfile = loadEnvVar("KAFKA_SPEC_FILE")
		if specfile == "" && actionApply {
			fmt.Println("Please define spec file with --spec option or with KAFKA_SPEC_FILE env variable")
			os.Exit(1)
		}
	}
	if protocol != "plaintext" {
		if username == "" {
			username = loadEnvVar("KAFKA_USERNAME")
		}
		if password == "" {
			password = loadEnvVar("KAFKA_PASSWORD")
		}
	}
}

func printVersion() error {
	fmt.Println(version)
	return nil
}

func usage() {
	usage := `Manage Kafka cluster resources (topics and ACLs)
Usage: %s <action> [<options>] [<broker connection options>]
    ----------------
    Actions
    --help           Show this help and exit
    --dump           Dump cluster resources and their configs to stdout
                     See also --json and --yaml options
    --apply          Idempotently align cluster resources with the spec manifest
                     See also --spec, --json and --yaml options
    --version        Show version
    ----------------
    Options
    --spec           A path to manifest (specification file) to be used
                     with --apply action
                     Can be also set by Env variable KAFKA_SPEC_FILE
    --yaml           Spec-file is in YAML format
                     Will try to detect format if none of --yaml or --json is set
    --json           Spec-file is in JSON format
                     Will try to detect format if none of --yaml or --json is set
    --template       Spec-file is a Go-template to be parsed. The values are read from
                     Env variables and from --var arguments (--var arguments are
                     taking precedence)
    --var            Variable in format "key=value". Can be presented multiple times
    --missingok      Do not fail if template key is not defined
    --verbose        Verbose output
    --stop-on-error  Exit on first occurred error
    ----------------
    Broker connection options
    --broker         Bootstrap-brokers, comma-separated. Default is localhost:9092
                     Can be also set by Env variable KAFKA_BROKER
    --protocol       Security protocol. Default is plaintext
                     Available options: plaintext, sasl_ssl, sasl_plaintext
    --mechanism      SASL mechanism. Default is scram-sha-256
                     Available options: scram-sha-256, scram-sha-512
    --username       Username for authentication
                     Can be also set by Env variable KAFKA_USERNAME
    --password       Password for authentication
                     Can be also set by Env variable KAFKA_PASSWORD
`

	fmt.Fprintf(os.Stderr, usage, os.Args[0])
	//flag.PrintDefaults()
	os.Exit(1)
}
