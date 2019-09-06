package main

import (
	"github.com/Shopify/sarama"

	"testing"
)

var aclOperationToStringTests = []struct {
	in  int
	out string
}{
	{0, "INVALID"},
	{1, "ANY"},
	{2, "ALL"},
	{3, "READ"},
	{4, "WRITE"},
	{5, "CREATE"},
	{6, "DELETE"},
	{7, "ALTER"},
	{8, "DESCRIBE"},
	{9, "CLUSTER_ACTION"},
	{10, "DESCRIBE_CONFIGS"},
	{11, "ALTER_CONFIGS"},
	{12, "IDEMPOTENT_WRITE"},
}

func TestAclOperationToString(t *testing.T) {
	for _, tt := range aclOperationToStringTests {
		val := aclOperationToString(sarama.AclOperation(tt.in))
		if val != tt.out {
			t.Errorf("aclOperationToString failed, expected %s, got %s", tt.out, val)
		}
	}
}

var aclOperationFromStringTests = []struct {
	in  string
	out int
}{
	{"ANY", 1},
	{"ALL", 2},
	{"READ", 3},
	{"WRITE", 4},
	{"CREATE", 5},
	{"DELETE", 6},
	{"ALTER", 7},
	{"DESCRIBE", 8},
	{"CLUSTER_ACTION", 9},
	{"DESCRIBE_CONFIGS", 10},
	{"ALTER_CONFIGS", 11},
	{"IDEMPOTENT_WRITE", 12},
}

func TestAclOperationFromString(t *testing.T) {
	for _, tt := range aclOperationFromStringTests {
		val := int(aclOperationFromString(tt.in))
		if val != tt.out {
			t.Errorf("aclOperationToString failed, expected %v, got %v", tt.out, val)
		}
	}
}

var aclResourcePatternTypeToStringTests = []struct {
	in  int
	out string
}{
	{0, "INVALID"},
	{1, "ANY"},
	{2, "MATCH"},
	{3, "LITERAL"},
	{4, "PREFIXED"},
}

func TestAclResourcePatternTypeToString(t *testing.T) {
	for _, tt := range aclResourcePatternTypeToStringTests {
		val := aclResourcePatternTypeToString(sarama.AclResourcePatternType(tt.in))
		if val != tt.out {
			t.Errorf("aclResourcePatternTypeToString failed, expected %s, got %s", tt.out, val)
		}
	}
}

var aclResourcePatternTypeFromStringTests = []struct {
	in  string
	out int
}{
	{"ANY", 1},
	{"MATCH", 2},
	{"LITERAL", 3},
	{"PREFIXED", 4},
}

func TestAclResourcePatternTypeFromString(t *testing.T) {
	for _, tt := range aclResourcePatternTypeFromStringTests {
		val := int(aclResourcePatternTypeFromString(tt.in))
		if val != tt.out {
			t.Errorf("aclResourcePatternTypeFromString failed, expected %v, got %v", tt.out, val)
		}
	}
}

var aclResourceTypeToStringTests = []struct {
	in  int
	out string
}{
	{0, "INVALID"},
	{1, "any"},
	{2, "topic"},
	{3, "group"},
	{4, "cluster"},
	{5, "transactional-id"},
}

func TestAclResourceTypeToString(t *testing.T) {
	for _, tt := range aclResourceTypeToStringTests {
		val := aclResourceTypeToString(sarama.AclResourceType(tt.in))
		if val != tt.out {
			t.Errorf("aclResourceTypeToString failed, expected %s, got %s", tt.out, val)
		}
	}
}

var aclResourceTypeFromStringTests = []struct {
	in  string
	out int
}{
	{"any", 1},
	{"topic", 2},
	{"group", 3},
	{"cluster", 4},
	{"transactional-id", 5},
}

func TestAclResourceTypeFromString(t *testing.T) {
	for _, tt := range aclResourceTypeFromStringTests {
		val := int(aclResourceTypeFromString(tt.in))
		if val != tt.out {
			t.Errorf("aclResourceTypeFromString failed, expected %v, got %v", tt.out, val)
		}
	}
}

var aclPermissionTypeToStringTests = []struct {
	in  int
	out string
}{
	{0, "INVALID"},
	{1, "ANY"},
	{2, "DENY"},
	{3, "ALLOW"},
}

func TestAclPermissionTypeToString(t *testing.T) {
	for _, tt := range aclPermissionTypeToStringTests {
		val := aclPermissionTypeToString(sarama.AclPermissionType(tt.in))
		if val != tt.out {
			t.Errorf("aclPermissionTypeToString failed, expected %s, got %s", tt.out, val)
		}
	}
}

var aclPermissionTypeFromStringTests = []struct {
	in  string
	out int
}{
	{"ANY", 1},
	{"DENY", 2},
	{"ALLOW", 3},
}

func TestAclPermissionTypeFromString(t *testing.T) {
	for _, tt := range aclPermissionTypeFromStringTests {
		val := int(aclPermissionTypeFromString(tt.in))
		if val != tt.out {
			t.Errorf("aclPermissionTypeFromString failed, expected %v, got %v", tt.out, val)
		}
	}
}
