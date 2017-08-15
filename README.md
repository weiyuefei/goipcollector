goIPCollector is a simple tool that collects IP data from ip.taobao.com and generates
records to insert into database.

Quick start
1) Install
go get github.com/apsdehal/go-logger
go get github.com/goless/config
go get github.com/mattn/go-sqlite3

go get github.com/weiyuefei/goipcollector

2) Run

go build github.com/weiyuefei/goipcollector/goIPCollector.go

cd github.com/weiyuefei/goipcollector

./goIPCollector config.json

TODO:
1) Support concurrent query
