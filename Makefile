.PHONY: rpm clean source test coverage
  
KAFKA_OPS_VERSION ?= 1.0.3
BUILD_NUMBER	  ?= 1
KAFKA_OPS         ?= kafka-ops
REPO              ?= github.com/agapoff/${KAFKA_OPS}

rpm: build
	@rpmbuild -v -bb \
	--define "version ${KAFKA_OPS_VERSION}" \
	--define "build_number ${BUILD_NUMBER}" \
	--define "_topdir %(pwd)/rpm-build" \
	--define "_sourcedir %(pwd)" \
	--define "_builddir %(pwd)" \
	rpm/kafka-ops.spec

clean:
	@rm -f kafka-ops
	@rm -rf rpm-build

test:
	go test -v -cover

coverage:
	go test -coverprofile=coverage.out
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

build: ${KAFKA_OPS}

${KAFKA_OPS}:
	go get ${REPO}
	go build -o ${KAFKA_OPS}
