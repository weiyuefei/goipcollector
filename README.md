goIPCollector
============
goIPCollector is a simple tool that collects IP data from ip.taobao.com and generates<br>
records to insert into database.

Feature
---------------
* support collecting ip info from ip.taobao.com
* support batch query

`Note`: The maximum qps that taobao supports is less than 10qps, <br>
thus, the batchNum in config.json should also be less than 10, or <br>
the unexpected results will be returned.

Quick start
--------------
### Installation
```Bash
* # go get github.com/apsdehal/go-logger<br>
* # go get github.com/goless/config<br>
* # go get github.com/mattn/go-sqlite3<br>
* # go get github.com/weiyuefei/goipcollector<br>
```

### Run
```Bash
* # go build github.com/weiyuefei/goipcollector/goIPCollector.go
* # cd github.com/weiyuefei/goipcollector
* # ./goIPCollector config.json
```

TODO list
* support resuming query when reboot
