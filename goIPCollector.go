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
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/goless/config"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {

	if len(os.Args) != 2 {
		log.Printf("Usage: %s config", os.Args[0])
		return
	}

	config := config.New(os.Args[1])

	//logFile := config.Get("logFile")
	file, err := os.Create(config.Get("logFile").(string))
	if err != nil {
		fmt.Errorf("Can't open log file, %s", err)
		return
	}
	defer file.Close()

	log, err := logger.New("goipcollector", 0, file)
	if err != nil {
		fmt.Errorf("Can't new log file, %s", err)
		return
	}

	// Create database and table
	if ok, err := InitDatabase(config.Get("dbFile").(string), log); !ok {
		log.FatalF("InitDatabase: %s", err)
		return
	}

	dataChan := make(chan string)
	var wg sync.WaitGroup

	wg.Add(2)
	go ReadIpSection(config.Get("ipFile").(string), dataChan, &wg, log)
	go ConsumeIpSection(config.Get("dbFile").(string), config.Get("urlBase").(string), int(config.Get("batchNum").(float64)), dataChan, &wg, log)

	fmt.Println("IP collector starts")
	wg.Wait()
}

// Create the database file and table
func InitDatabase(dbFile string, log *logger.Logger) (bool, error) {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return false, err
	}
	defer db.Close()

	sql := `CREATE TABLE IF NOT EXISTS ip_warehouse(country TEXT, country_id TEXT, area, area_id INTEGER, provice TEXT, provice_id INTEGER, city TEXT, city_id INTEGER, isp TEXT, isp_id INTEGER, ip TEXT);`
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

func ConsumeIpSection(dbFile, urlBase string, batchNum int, dataChan chan string, wg *sync.WaitGroup, log *logger.Logger) {

	defer wg.Done()

	for {
		record, ok := <-dataChan
		if !ok {
			break
		}
		log.InfoF("Consuming record[%s]", record)

		startTime := time.Now().Second()

		DoConsume(record, dbFile, urlBase, batchNum, log)

		endTime := time.Now().Second()

		log.InfoF("Consuming time %d seconds", endTime-startTime)
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

func DoConsume(dataRecord, dbFile, urlBase string, batchNum int, log *logger.Logger) {
	items, err := SplitRecord(dataRecord)
	if err != nil {
		log.ErrorF("SplitRecord: %s", err)
		return
	}

	ipCount, err := strconv.Atoi(items[3])
	if err != nil {
		log.ErrorF("Atoi: %s", errors.New("Invalid IP count number"))
		return
	}

	startIPNums := strings.Split(items[0], ".")
	E := errors.New("Not A IP.")
	if len(startIPNums) != 4 {
		log.ErrorF("Split: %s", E)
		return
	}
	var startIPNet int
	for k, v := range startIPNums {
		num, err := strconv.Atoi(v)
		if err != nil || num > 255 {
			log.ErrorF("Atoi: %s", E)
			return
		}
		startIPNet = startIPNet | num<<uint(8*(3-k))
	}

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.ErrorF("Open %s: %s", dbFile, err)
		return
	}
	defer db.Close()

	for i := 0; i < ipCount; i += batchNum {

		var wg sync.WaitGroup
		
		for j := 0; j < batchNum && (i+j) < ipCount; j++ {
			wg.Add(1)
			go QueryAndSave(urlBase, i+j+startIPNet, &wg, db, log)
		}

		wg.Wait()
	}
}

func QueryAndSave(urlBase string, ipnr int, wg *sync.WaitGroup, db *sql.DB, log *logger.Logger) {

	defer wg.Done()

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
			return
		}

		break
	}

	if code == 104 {
		log.ErrorF("Connection was always reset by peer, IP=%s", IPStr(ipnr))
		return
	}

	log.InfoF("####%s####", data)
	dat := ParseJson(data)
	if dat == nil {
		log.ErrorF("ParseJson error, skip, IP=%s", IPStr(ipnr))
		return
	}

	stmt, err := db.Prepare("INSERT INTO ip_warehouse(country,country_id,area,area_id,provice,provice_id,city,city_id,isp,isp_id,ip) values(?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		log.ErrorF("Prepare@@@@@@%s@@@@@@", data)
		return
	}

	area_id, _ := strconv.Atoi(dat["area_id"])
	provice_id, _ := strconv.Atoi(dat["region_id"])
	city_id, _ := strconv.Atoi(dat["city_id"])
	isp_id, _ := strconv.Atoi(dat["isp_id"])

	_, err = stmt.Exec(dat["country"], dat["country_id"], dat["area"], area_id,
		dat["region"], provice_id, dat["city"], city_id,
		dat["isp"], isp_id, dat["ip"])
	if err != nil {
		log.ErrorF("@@@@@@Exec Error@@@@@@")
	}
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
	    return 2, err.String()
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
