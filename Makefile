SHELL := /bin/bash

# ================================================
# Config
# ================================================

GO:=$(GOROOT)/bin/go
export PATH:=$(GOROOT)/bin:$(PATH)

ENV               ?= dev
PROJECT_NAME      := transientvariable/lettuce
COMMIT            := $(shell git rev-parse --short HEAD)
BIN_NAME          := lettuce
BUILD_TIMESTAMP   := $(date -u +'%Y-%m-%dT%H:%M:%SZ')
BUILD_OUTPUT_DIR  := build

MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --silent

# ================================================
# Rules
# ================================================

default: all

.PHONY: all
all: clean check build

.PHONY: clean
clean:
	@printf "\033[2m→ Cleaning project build output directory: $(BUILD_OUTPUT_DIR)\033[0m\n"
	@rm -rf "$(BUILD_OUTPUT_DIR)" 2> /dev/null

.PHONY: check
check:
	@printf "\033[2m→ No checks for this repository at this time...\033[0m\n"

.PHONY: build.all
build.all: clean build

.PHONY: build
build:
	@printf "\033[2m→ Building application binary...\033[0m\n"
	@mkdir -p $(BUILD_OUTPUT_DIR)
	@go get -d -v ./...
	@go build -installsuffix 'static' -o $(BUILD_OUTPUT_DIR)/$(BIN_NAME) .