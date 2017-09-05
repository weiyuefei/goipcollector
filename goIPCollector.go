// Author(@)Feeman(weiyuefei@gmail.com)
//
// Project goIPCollector implements simple tool which collects ip info
// from ip.taobao.com and inserts records into database.
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/goless/config"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {

	rootPathPtr := flag.String("p", "./", "The prefix path")
	configFilePtr := flag.String("c", "conf/config.json", "The config file")
	signalNamePtr := flag.String("s", "", "The reload|stop signal")

	flag.Parse()

	configAbsPath := *rootPathPtr + *configFilePtr
	config := config.New(configAbsPath)

	if *signalNamePtr != "" {
		pid, err := GetPid(config.Get("pid").(string))
		if err != nil {
			fmt.Printf("Error:%s\n", err.Error())
			return
		}

		proc, err := os.FindProcess(pid)
		if err != nil {
			fmt.Printf("Error:%s\n", err.Error())
			return
		}

		var sig os.Signal
		switch *signalNamePtr {
		case "reload":
			sig = syscall.SIGUSR2
		case "stop":
			sig = os.Interrupt
		}

		proc.Signal(sig)
		return
	}

	logFile, err := os.OpenFile(config.Get("logFile").(string), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0)
	if err != nil {
		fmt.Errorf("Can't open log file, %s", err)
		return
	}
	defer logFile.Close()

	log, err := logger.New("goipcollector", 0, logFile)
	if err != nil {
		fmt.Errorf("Can't new log file, %s", err)
		return
	}

	// Create database and table
	if ok, err := InitDatabase(config.Get("dbFile").(string), log); !ok {
		log.FatalF("InitDatabase: %s", err)
		return
	}

	pidFile, err := os.OpenFile(config.Get("pid").(string), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.ErrorF("Open pidfile error: %v", err)
		return
	}

	info, _ := pidFile.Stat()
	if info.Size() != 0 {
		log.InfoF("%s is already running", os.Args[0])
		return
	}

	if config.Get("daemon").(string) == "on" && os.Getppid() != 1 {
		Daemon()
	}

	pidFile.WriteString(fmt.Sprint(os.Getpid()))
	go Worker(&config, log)
	Master(pidFile, log)
}

func GetPid(pidfile string) (int, error) {
	data, err := ioutil.ReadFile(pidfile)
	if err != nil {
		return 0, err
	}

	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, err
	}

	return pid, nil
}

func Master(pidFile *os.File, log *logger.Logger) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGUSR2)
	for {
		s := <-c
		switch s {
		case syscall.SIGUSR2:
			log.InfoF("receive SIGUSR2 to reload")
		case os.Interrupt:
			log.InfoF("receive SIGINT to stop")
			pidFile.Close()
			os.Remove(pidFile.Name())
			fmt.Println("bye")
			os.Exit(0)
		}
	}
}

func Worker(config *config.Config, log *logger.Logger) {
	batchNum := config.Get("batchNum").(float64)
	dataChan := make(chan string, int(batchNum))

	var wg sync.WaitGroup
	wg.Add(1)
	go ReadIpSection(config.Get("ipFile").(string), dataChan, &wg, log)

	for i := 0; i < int(batchNum); i++ {
		wg.Add(1)
		go ConsumeIpSection(config.Get("dbFile").(string), config.Get("urlBase").(string), dataChan, &wg, log)
	}

	log.InfoF("Application starts success")
	wg.Wait()
}

func Daemon() {
	args := append([]string{os.Args[0]}, os.Args[1:]...)
	os.StartProcess(os.Args[0], args, &os.ProcAttr{Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}})
	os.Exit(0)
}

// Create the database file and table
func InitDatabase(dbFile string, log *logger.Logger) (bool, error) {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return false, err
	}
	defer db.Close()

	sql := `CREATE TABLE IF NOT EXISTS ip_warehouse(country TEXT, country_id TEXT, area, area_id INTEGER, region TEXT, region_id INTEGER, city TEXT, city_id INTEGER, isp TEXT, isp_id INTEGER, start_ip INTEGER,end_ip INTEGER,start_ipa TEXT, end_ipa TEXT);`
	db.Exec(sql)

	return true, nil
}

func ReadIpSection(ipFile string, dataChan chan string, wg *sync.WaitGroup, log *logger.Logger) {
	// when finish or error, close the channel to tell
	// consumer to quit and minus the mutex counter.
	defer close(dataChan)
	defer wg.Done()

	fi, err := os.Open(ipFile)
	if err != nil {
		log.ErrorF("Open %s: %s", ipFile, err)
		return
	}
	defer fi.Close()

	br := bufio.NewReader(fi)
	for {
		bytes, _, err := br.ReadLine()
		if err == io.EOF {
			log.InfoF("Read \"%s\" finished", ipFile)
			return
		}

		dataChan <- string(bytes)
	}
}

func ConsumeIpSection(dbFile, urlBase string, dataChan chan string, wg *sync.WaitGroup, log *logger.Logger) {

	defer wg.Done()

	for {
		t1 := time.Now()

		record, ok := <-dataChan
		if !ok {
			break
		}

		log.InfoF("Consuming record[%s]", record)
		DoConsume(record, dbFile, urlBase, log)

		t2 := time.Now()
		log.InfoF("Consuming time: %v, record[%s]", t2.Sub(t1), record)
	}
}

func SplitRecord(record string) ([]string, error) {

	var res []string

	s := -1
	for i := 0; i < len(record); i++ {

		if record[i] == '.' || record[i] == '/' || (record[i] >= '0' && record[i] <= '9') {
			if s == -1 {
				s = i
			}
			continue
		}
		if s != -1 {
			res = append(res, string(record[s:i]))
			s = -1
		}
	}

	// The last one to be appended
	res = append(res, string(record[s:len(record)]))

	if len(res) != 4 {
		return res, errors.New("Illegal record")
	}

	return res, nil
}

type QueryRecord struct {
	country   string
	countryId string
	area      string
	areaId    int
	region    string
	regionId  int
	city      string
	cityId    int
	isp       string
	ispId     int
}

func DoConsume(dataRecord, dbFile, urlBase string, log *logger.Logger) {
	items, err := SplitRecord(dataRecord)
	if err != nil {
		log.ErrorF("SplitRecord: %s", err)
		return
	}

	startIPNet, err := IPNum(items[0])
	if err != nil {
		log.ErrorF("Start IPNum: %s", err)
		return
	}

	endIPNet, err := IPNum(items[1])
	if err != nil {
		log.ErrorF("End IPNum: %s", err)
		return
	}

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.ErrorF("Open %s: %s", dbFile, err)
		return
	}
	defer db.Close()

	rangeStartIP := startIPNet
	cacheR := make(map[int]*QueryRecord)

	var (
		targetRecord *QueryRecord
		ok           bool
		rc           int
	)

	for rangeStartIP <= endIPNet {

		targetRecord, ok = cacheR[rangeStartIP]
		if !ok {
			rc, targetRecord = TryQuery(urlBase, rangeStartIP, log)
			if targetRecord == nil {
				if rc != 104 {
					rangeStartIP += 1
				}
				continue
			}
		}

		low := rangeStartIP
		high := endIPNet
		mid := low + (high-low)/2

		// binaray search
		for low <= high {
			midRecord, ok := cacheR[mid]
			if !ok {
				_, midRecord = TryQuery(urlBase, mid, log)
				if midRecord == nil {
					break
				}
			}

			if RecordIsEqual(targetRecord, midRecord) {
				low = mid + 1
			} else {
				high = mid - 1
				cacheR[mid] = midRecord
			}

			mid = low + (high-low)/2
		}

		// range record [rangeStartIP, mid]
		SaveToDB(db, targetRecord, rangeStartIP, mid-1, log)

		// update cache
		for key, _ := range cacheR {
			if key < mid {
				delete(cacheR, key)
			}
		}

		rangeStartIP = mid
	}
}

func RecordIsEqual(r1, r2 *QueryRecord) bool {

	if r1.countryId != r2.countryId {
		return false
	}

	if r1.countryId == "HK" || r1.countryId == "TW" ||
		r1.countryId == "MO" {
		return true
	}

	if r1.regionId != r2.regionId ||
		r1.areaId != r2.areaId ||
		r1.cityId != r2.cityId ||
		r1.ispId != r2.ispId {
		return false
	}

	return true
}

func SaveToDB(db *sql.DB, rec *QueryRecord, startIP, endIP int, log *logger.Logger) {

	stmt, err := db.Prepare("INSERT INTO ip_warehouse(country,country_id,area,area_id,region,region_id,city,city_id,isp,isp_id,start_ip, end_ip, start_ipa, end_ipa) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		log.ErrorF("###### SaveToDB Prepare error")
		return
	}

	_, err = stmt.Exec(rec.country, rec.countryId, rec.area, rec.areaId,
		rec.region, rec.regionId, rec.city, rec.cityId,
		rec.isp, rec.ispId, startIP, endIP, IPStr(startIP), IPStr(endIP))
	if err != nil {
		log.ErrorF("###### SaveToDB Exec error")
	}
}

func TryQuery(urlBase string, ipnr int, log *logger.Logger) (int, *QueryRecord) {

	var code int
	var data string

	maxTryTimes := 5

	for try := 0; try < maxTryTimes; try++ {

		code, data = DoQuery(urlBase, ipnr, log)
		if code == 104 {
			log.ErrorF("Connection was reset by peer (%d) times, IP=%s",
				try+1, IPStr(ipnr))
			time.Sleep(time.Duration(math.Pow(2, float64(try))) * time.Second)
			continue
		}

		if code != 200 && code != 206 {
			log.ErrorF("StatusCode=%d, IP=%s", code, IPStr(ipnr))
			return code, nil
		}

		break
	}

	if code == 104 {
		log.ErrorF("Connection was always reset by peer, IP=%s", IPStr(ipnr))
		return code, nil
	}

	log.InfoF("####%s####", data)
	dat := ParseJson(data)
	if dat == nil {
		log.ErrorF("ParseJson error, skip, IP=%s", IPStr(ipnr))
		return 1, nil
	}

	rec := &QueryRecord{country: dat["country"], countryId: dat["country_id"],
		area: dat["area"], region: dat["region"], city: dat["city"], isp: dat["isp"]}

	rec.areaId, _ = strconv.Atoi(dat["area_id"])
	rec.regionId, _ = strconv.Atoi(dat["region_id"])
	rec.cityId, _ = strconv.Atoi(dat["city_id"])
	rec.ispId, _ = strconv.Atoi(dat["isp_id"])

	return code, rec
}

func ParseJson(data string) map[string]string {
	var dat map[string]interface{}
	if err := json.Unmarshal([]byte(data), &dat); err != nil {
		return nil
	}

	md, ok := dat["data"].(map[string]interface{})
	if !ok {
		return nil
	}

	rtnValue := make(map[string]string)
	for k, v := range md {
		vv := v.(string)
		if vv == "" {
			rtnValue[k] = "-"
		} else {
			rtnValue[k] = vv
		}
	}

	return rtnValue
}

func DoQuery(urlBase string, ipNet int, log *logger.Logger) (int, string) {

	client := &http.Client{}

	ipStr := IPStr(ipNet)

	url := fmt.Sprintf("%s%s", urlBase, ipStr)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.ErrorF("NewRequest: %s", err)
		return 0, ""
	}

	req.Header.Add("User-Agent", "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)")
	resp, err := client.Do(req)
	if err != nil {
		log.ErrorF("Response returns nil: %s", err)
		return 2, err.Error()
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		log.ErrorF("StatusCode=%d", resp.StatusCode)
		return resp.StatusCode, ""
	}

	defer func() {
		if r := recover(); r != nil {
			log.ErrorF("GET panic info, recover it, err=%s", r)
		}
	}()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.ErrorF("Read resp.body error, err=%s", err)
		return 1, ""
	}

	return resp.StatusCode, string(body)
}

func IPStr(ipnr int) string {
	var bytes [4]byte
	bytes[0] = byte(ipnr & 0xFF)
	bytes[1] = byte((ipnr >> 8) & 0xFF)
	bytes[2] = byte((ipnr >> 16) & 0xFF)
	bytes[3] = byte((ipnr >> 24) & 0xFF)

	return net.IPv4(bytes[3], bytes[2], bytes[1], bytes[0]).String()
}

func IPNum(ipaddr string) (int, error) {

	IPNums := strings.Split(ipaddr, ".")
	err := errors.New("Not A IPv4 format.")
	if len(IPNums) != 4 {
		return 0, err
	}

	var ipnr int
	for k, v := range IPNums {
		num, err := strconv.Atoi(v)
		if err != nil || num > 255 {
			return 0, err
		}

		ipnr = ipnr | num<<uint(8*(3-k))
	}

	return ipnr, nil
}

