# Redis-golang
***
This is a project for creating a Redis database using the Go programming language.

## Links 
***
- [Setup](#setup)
- [Use](#use)
- [Commands](#commands)

### Setup
***
To set up project:
1. Clone the repository:
```
    git clone https://github.com/Z1TK/redis-golang.git
```
2. Stop a Redis
```
    sudo systemctl stop redis
```
3. Run:
```
    go run *.go
```

### Use
***
Use following command to connect to redis-golang server
```
redis-cli
```

### Commands
***
redis-golang supports the following commands:
1. String
```
    SET, GET, SETNX, SETEX, GETEX, STRLEN, GETRANGE, MSET, MGET, INCR, DECR
```
2. Hash
```
    HSET, HGET, HDEL, HEXISTS, HMGET, HGETALL, HLEN, HKEYS, HVALS
```
3. List
```
    RPUSH, LPUSH, RPOP, LPOP, LRANGE, LPUSHX, RPUSHX, LLEN
```
4. Generic
```
    DEL, EXPIRE, TTL
```
5. Connection
```
    PING
```