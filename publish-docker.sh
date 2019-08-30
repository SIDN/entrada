#!/usr/bin/env bash

# build image and add the version + latest tag
# upload both tags to docker hub

mvn dockerfile:build
mvn dockerfile:tag@tag-version
mvn dockerfile:push@push-latest
mvn dockerfile:push@push-version

