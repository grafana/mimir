#!/bin/bash -eu

BINARY_READER=BinaryReader go test -count=5 -v -benchmem -bench=BenchmarkOpsBinaryReader -run=BenchmarkOpsBinaryReader | tee bench.BinaryReader.txt
BINARY_READER=ReadBinaryReader go test -count=5 -v -benchmem -bench=BenchmarkOpsBinaryReader -run=BenchmarkOpsBinaryReader | tee bench.ReadBinaryReader.txt
BINARY_READER=PopulateBinaryReader go test -count=5 -v -benchmem -bench=BenchmarkOpsBinaryReader -run=BenchmarkOpsBinaryReader | tee bench.PopulateBinaryReader.txt

echo BinaryReader vs ReadBinaryReader
benchstat bench.BinaryReader.txt bench.ReadBinaryReader.txt

echo BinaryReader vs PopulateBinaryReader
benchstat bench.BinaryReader.txt bench.PopulateBinaryReader.txt
