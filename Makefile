.PHONY: all configure debug release test clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=raster
USE_UNSTABLE_C_API=0
TARGET_DUCKDB_VERSION=v1.5.4

all: configure release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

SKIP_TESTS=1

clean: clean_build clean_rust
clean_all: clean clean_configure
