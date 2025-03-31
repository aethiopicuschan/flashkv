# FlashKV

[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen?style=flat-square)](/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/aethiopicuschan/flashkv)](https://goreportcard.com/report/github.com/aethiopicuschan/flashkv)
[![CI](https://github.com/aethiopicuschan/flashkv/actions/workflows/ci.yaml/badge.svg)](https://github.com/aethiopicuschan/flashkv/actions/workflows/ci.yaml)

FlashKV is a fast, memcached-compatible key-value store.

## Benchmark vs. memcached

I created a tool called [memcached-checker](https://github.com/aethiopicuschan/memcached-checker) and added functionality to benchmark `Set`, `Get`, and `Del`. Below is a comparison of the average values from 5 runs. The benchmarks were run on a MacBookPro M3 Max with 36GB of memory.

| Application | Set (QPS) | Get (QPS) | Del (QPS) |
| ----------- | --------- | --------- | --------- |
| FlashKV     | 11637.54  | 11337.09  | 11682.99  |
| Memcached   | 10803.24  | 10605.32  | 10842.90  |

It is approximately 1.1x faster!

## Installation

```sh
go install github.com/aethiopicuschan/flashkv@latest
```
