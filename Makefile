GO        := go
GOBUILD   := CGO_ENABLED=0 $(GO) build
LDFLAGS += -X "github.com/bytewatch/dolphinbeat/util.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/bytewatch/dolphinbeat/util.GitHash=$(shell git rev-parse --short HEAD)"

.PHONY: all dolphinbeat kafka-consumer

all: dolphinbeat kafka-consumer

dolphinbeat:
	mkdir -p build/dolphinbeat/bin && mkdir -p build/dolphinbeat/etc
	cd cmd/dolphinbeat/ && $(GOBUILD) -ldflags '$(LDFLAGS)'  -o ../../build/dolphinbeat/bin/dolphinbeat
	cp -r cmd/dolphinbeat/dolphinbeat.toml.* build/dolphinbeat/etc/

kafka-consumer:
	cd cmd/tools/kafka-consumer && $(GOBUILD) -ldflags '$(LDFLAGS)'  -o ../../../build/dolphinbeat/bin/kafka-consumer
