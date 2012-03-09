#!/usr/bin/env bash
protoc --java_out=../frontend dragent.proto
protoc --java_out=../frontend dtxn.proto