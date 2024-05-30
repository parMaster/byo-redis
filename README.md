[![progress-banner](https://backend.codecrafters.io/progress/redis/ac455f08-a075-4a3a-a72b-553df7d15520)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# CodeCrafters Redis Challenge
Golang implementation of the redis server for the CodeCrafters Redis Challenge.

## Features
Supports `GET`, `PING`, `SET`, `INFO` commands for Redis protocol. Can work with multiple replicas and supports simple propagation of data from master to replicas.

## Thigs I learned from this challenge
- How to use `net` package to create a TCP server and client in Go.
- How redis protocol works, basic knowledge of redis replication principles (`REPLCONF`, `PSYNC` commands) and how to implement them. Including handshake, data propagation to replicas.
- How to implement client-server (master-replicas) communication using TCP sockets and redis protocol.
- How to write tests for the server and client, testing the server using the client.
- How to use `bufio` package to read and write data from and to the TCP connection.
- Intricate details of TCP communication, how it does not guarantee the order of the data, doesn't have a concept of message boundaries (you have to define them yourself - basically make your own protocol on top of TCP).
- How to debug really complex cases of data races in concurrent code involving multiple servers and clients running in parallel.
- My [mcache](http://github.com/parMaster/mcache) package turned out to be very useful for this challenge.
- That CodeCrafters challenges are really fun and challenging to work on, provides just right amount of guidance and freedom to explore and learn new things, perfect gradient of difficulty for me. A bit expensive though, but this one was free this month.