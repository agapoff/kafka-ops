# kafka-ops

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/agapoff/kafka-ops/blob/master/LICENSE)

Yet another CLI utility to automate Kafka cluster management

**Kafka-Ops** is a command-line utility written in Go and mostly inspired by [KafkaSpecs](https://github.com/streamthoughts/kafka-specs) java tool. It allows to automate Kafka management by describing resources (topics, ACLs) and their configs in spec-files and applying them to Kafka cluster. Kafka-Ops uses AdminClient Kafka API to align cluster resources with the spec idempotently. The spec can be manifested either in YAML or JSON format.


## Requirements

* Kafka 2.0+

## Spec Files Examples

Both YAML and JSON formats of spec files have the same notation and can be converted from each other. This is the example for YAML format:

kafka-cluster-example1.yaml:
```yaml
---
topics:
- configs:
    cleanup.policy: compact
    compression.type: producer
    min.insync.replicas: '1'
    retention.ms: 'default'
  name: my-topic1
  partitions: 3
- name: my-topic2
  state: absent
- name: my-topic3
  partitions: 1
  replication_factor: 1
  configs:
      min.insync.replicas: 'default'
      retention.ms: 30000
acls:
  - principal: 'User:test1'
    permissions:
    - resource:
        type: 'topic'
        pattern: 'my-'
        patternType: 'PREFIXED'
      allow_operations: ['READ:*', 'WRITE:*', 'DESCRIBE:*']
    - resource:
        type: 'group'
        pattern: 'my-group'
        patternType: 'LITERAL'
      allow_operations: ['READ:*']
      deny_operations: ['DESCRIBE:*']
  - principal: '*'
    permissions:
    - resource:
        type: 'topic'
        pattern: 'my-topic'
        patternType: 'MATCH'
      allow_operations: ['ANY']
      state: absent
```

The format is quite evident. Just few remarks:
* The topic config values are always strings, while *partitions* and *replication_factor* are always numeric
* The topic config value can be set to *default*. This will remove the per-topic setting and the topic will be using the cluster default value
* *replication_factor* for topic is optional. If utility will need to create the topic and this setting will not be defined then it will be set to 1 on single-node clusters and to 2 on multi-node clusters
* The parameter *state=absent* can be used for deleting topics and ACLs if they present. Any value other than *absent* is considered as *present*
* The *patternType=MATCH*, *patternType=ANY*, *operation=ANY*, *principal=&ast;* can be used when *state=absent* for deleting ACLs but be careful with that
* The ACL operation is described as *OperationType:Host*
* The Host part can be omitted and will be considered as '&ast;' when *state=present* and as any host (including '&ast;' itself and any separately defined IP) when *state=absent*

kafka-cluster-example2.json:
```json
{
    "topics": [
        {
            "name": "my-topic2",
            "partitions": 3,
            "replication_factor": 1,
            "configs": {
                "retention.ms": "30000",
                "segment.bytes": "1073741824"
            }
        }
    ],
    "acls": [
        {
            "principal": "User:ANONYMOUS",
            "permissions": [
                {
                    "resource": {
                        "type": "group",
                        "pattern": "*",
                        "patternType": "LITERAL"
                    },
                    "allow_operations": [
                        "READ:*"
                    ]
                },
                {
                    "resource": {
                        "type": "topic",
                        "pattern": "*",
                        "patternType": "LITERAL"
                    },
                    "allow_operations": [
                        "ALL:*",
                        "WRITE:*",
                        "DESCRIBE:*",
                        "READ:*",
                        "CREATE:*"
                    ]
                },
                {
                    "resource": {
                        "type": "cluster",
                    },
                    "allow_operations": [
                        "IDEMPOTENT_WRITE:*"
                    ]
                }
            ]
        }
    ]
}
```

## How to Apply the Spec

```bash
./kafka-ops --apply --broker kafka1.cluster.local:9092,kafka2.cluster.local:9092 --spec kafka-cluster-example1.yaml --yaml
```

output:
```
TASK [TOPIC : Create topic my-topic1 (partitions=3, replicas=1)] ***********************
ok: [kafka1.cluster.local:9092]

TASK [TOPIC : Delete topic my-topic2 ***************************************************
ok: [kafka1.cluster.local:9092]

TASK [TOPIC : Create topic my-topic3 (partitions=1, replicas=1)] ***********************
ok: [kafka1.cluster.local:9092]

TASK [ACL : Create ACL (ALLOW User:test1@* to READ topic:PREFIXED:my-)] ****************
changed: [kafka1.cluster.local:9092]

TASK [ACL : Create ACL (ALLOW User:test1@* to WRITE topic:PREFIXED:my-)] ***************
changed: [kafka1.cluster.local:9092]

TASK [ACL : Create ACL (ALLOW User:test1@* to DESCRIBE topic:PREFIXED:my-)] ************
changed: [kafka1.cluster.local:9092]

TASK [ACL : Create ACL (ALLOW User:test1@* to READ group:LITERAL:my-group)] ************
changed: [kafka1.cluster.local:9092]

TASK [ACL : Create ACL (DENY User:test1@* to DESCRIBE group:LITERAL:my-group)] *********
changed: [kafka1.cluster.local:9092]

SUMMARY ********************************************************************************

 ok=3    changed=5    failed=0
```

Some settings can be read from environment variables:
```bash
export KAFKA_BROKER=kafka1.cluster.local:9093
export KAFKA_SPEC_FILE=kafka-cluster-example2.json
export KAFKA_USERNAME=admin
export KAFKA_PASSWORD=admin-secret
./kafka-ops --apply --protocol sasl_ssl --json --verbose --stop-on-error
```

## How to Dump the Current Cluster Config

Kafka-Ops can also export the current topics and ACLs from the cluster. This can be useful for editing the spec and applyting back or for migrating the spec to another cluster.

```bash
./kafka-ops --dump --yaml
```

Note that if no broker is defined then Kafka-Ops tries to connect to *localhost:9092*.


## Templating

Kafka-Ops supports the simple templating for Spec-file. For now the variables are only read from environment variables and from command-line arguments.

Templating can be useful for multi-tenant and multi-environment Kafka clusters.

kafka-cluster-example3.yaml
```yaml
---
topics:
- configs:
    cleanup.policy: compact
  name: my-product.{{ .Plant }}.{{ .Env }}.my-topic
  partitions: 2
```

This spec can be then applied:

```
Plant=myplant Env=myenv ./kafka-ops --apply --spec kafka-cluster-example3.yaml --template --var Env=realenv --var One=more

TASK [TOPIC : Create topic my-product.myplant.realenv.my-topic (partitions=2, replicas=1)] ****************
changed: [cy-selenium.quotix.io:9092] 

SUMMARY ********************************************************************************
 ok=0    changed=1    failed=0
```

The value defined in command-line argument takes precedence over the one from environment variable.

All go-template functions can be used. Example:

```
name: my-product.{{ if .Plant }}{{ .Plant }}{{ else }}default{{ end }}.{{ .Env }}.my-topic
```

But Kafka-Ops fails if some unresolved template key is encountered. In order to override this behaviour use flag *--missingok*.


## Full Usage

```
 ./kafka-ops --help
Manage Kafka cluster resources (topics and ACLs)
Usage: ./kafka-ops <action> [<options>] [<broker connection options>]
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
```

## How to build the binary

You need golang and GNU make to be installed.

```bash
make test && make build
```

If you have rpm-build installed then you can build RPM-package

```bash
make rpm
```

## Contributing

This is an open source project so feel free to contribute.
