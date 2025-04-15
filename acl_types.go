package main

import (
	"github.com/IBM/sarama"

	"strings"
)

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
		return sarama.AclResourceUnknown
	}
}

func aclPermissionTypeToString(permissionType sarama.AclPermissionType) string {
	switch permissionType {
	case sarama.AclPermissionAny:
		return "ANY"
	case sarama.AclPermissionDeny:
		return "DENY"
	case sarama.AclPermissionAllow:
		return "ALLOW"
	default:
		return "INVALID"
	}
}

func aclPermissionTypeFromString(permissionType string) sarama.AclPermissionType {
	switch strings.ToUpper(permissionType) {
	case "ALLOW":
		return sarama.AclPermissionAllow
	case "DENY":
		return sarama.AclPermissionDeny
	case "ANY":
		return sarama.AclPermissionAny
	default:
		return sarama.AclPermissionUnknown
	}
}
