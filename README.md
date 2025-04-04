# FlashKV

[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen?style=flat-square)](/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/aethiopicuschan/flashkv)](https://goreportcard.com/report/github.com/aethiopicuschan/flashkv)
[![CI](https://github.com/aethiopicuschan/flashkv/actions/workflows/ci.yaml/badge.svg)](https://github.com/aethiopicuschan/flashkv/actions/workflows/ci.yaml)

FlashKV is a fast, memcached-compatible key-value store.

## Installation

```sh
go install github.com/aethiopicuschan/flashkv@latest
```

## Extra commands for persistence

Persistence of the store is possible with the following command.

### Save

`save {path}`

### Load

`load {path}`

## Benchmark

To compare [memcached](https://memcached.org/) and [DragonFly](https://github.com/dragonflydb/dragonfly), I ran a benchmark with the following command.
Each application is executed using Docker on a MacBook Pro equipped with an M3 Max and 32GB of memory.

```sh
memtier_benchmark -s 127.0.0.1 -p 11211 --protocol=memcache_text -c 50 -n 100000 --threads=4
```

### SET Benchmark

| Application | QPS     | Latency 99% | Latency 99.9% |
| ----------- | ------- | ----------- | ------------- |
| FlashKV     | 9173.54 | 4.447ms     | 6.943ms       |
| Memcached   | 9101.54 | 4.575ms     | 8.031ms       |
| Dragonfly   | 8164.26 | 5.023ms     | 7.999ms       |

### GET Benchmark

| Application | QPS       | Latency 99% | Latency 99.9% |
| ----------- | --------- | ----------- | ------------- |
| FlashKV     | 91734.42  | 4.447ms     | 6.847ms       |
| Memcached   | 91014.43  | 4.543ms     | 7.647ms       |
| Dragonfly   | 81641.65  | 4.991ms     | 7.871ms       |

In every category, FlashKV achieved outstanding results.
