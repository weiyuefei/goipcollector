goIPCollector
============
goIPCollector is a simple tool that collects IP data from ip.taobao.com and generates<br>
records to insert into database.

Quick start
--------------
### Installation
* go get github.com/apsdehal/go-logger<br>
* go get github.com/goless/config<br>
* go get github.com/mattn/go-sqlite3<br>
* go get github.com/weiyuefei/goipcollector<br>

### Run
* go build github.com/weiyuefei/goipcollector/goIPCollector.go
* cd github.com/weiyuefei/goipcollector
* ./goIPCollector config.json

TODO List
------------
* Support concurrency
