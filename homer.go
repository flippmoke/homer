
package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/xml"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"math"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/streadway/amqp"
	_ "github.com/mattn/go-sqlite3"
)

type Settings struct {
	XMLName      xml.Name `xml:"Server"`
	Debug        bool     `xml:"debug,attr"`
	Swarm	     bool     `xml:"swarm,attr"`
	IPDB         struct {
		File      string `xml:",attr"`
		CacheSize string `xml:",attr"`
	}
	AMQP struct {
		Host	    string `xml:",attr"`
		Exchange    string `xml:",attr"`
		Workers     int    `xml:",attr"`
	}
}

type Message struct {
	User_ip		string
	User_host	string
	Lat		float64
	Lon		float64
	Timestamp	string
	Method		string
	Url		string
	Response_code	string
	Response_bytes	string
	Response_time	string
	Referer		string
	User_agent	string
}

type Swarm_Message struct {
	User_ip		string
	User_host	string
	Lat		float64
	Lon		float64
	Timestamp	string
	Method		string
	Url		string
	Response_code	string
	Response_bytes	string
	Response_time	string
	Referer		string
	User_agent	string
	Request_type	string
	Products	string
	Request_lat	float64
	Request_lon	float64
}

var conf *Settings
var rxp *regexp.Regexp
var rxp_comptile *regexp.Regexp
var rxp_tile *regexp.Regexp
var rxp_validframes *regexp.Regexp
func main() {
	
	var conf_loc string;
        flag.StringVar(&conf_loc, "c", "homer.cfg", "Specifies the config file location")
	flag.Parse()

	if buf, err := ioutil.ReadFile(conf_loc); err != nil {
		panic(err)
	} else {
		conf = &Settings{}
		if err := xml.Unmarshal(buf, conf); err != nil {
			panic(err)
		}
	}
	reader := bufio.NewReader(os.Stdin)
	c := make(chan amqp.Publishing, 300)
	for i := 0; i < conf.AMQP.Workers; i++ {
		go worker(c, conf.AMQP.Host, conf.AMQP.Exchange)
	}

	db, err := sql.Open("sqlite3", conf.IPDB.File)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("PRAGMA cache_size=" + conf.IPDB.CacheSize)
	if err != nil {
		panic(err)
	}
	rxp = regexp.MustCompile(`"?(?P<user_ip>[^",]*)[^"]*"?\s?(?P<user_host>\S+)? \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>[A-Z]+)\s(?P<url>[^\s]*)\s[^"]*" (?P<response_code>\S+) (?P<response_bytes>\S+) (?P<response_time>\S+)?\s?"(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"`)
	rxp_comptile = regexp.MustCompile(`/swarmweb/comptile/(?:(?P<z>\d+)/(?P<x>\d+)/(?P<y>\d+)\.)?(?P<bing>\d+\.)?\S+(?:(?:LAYERS|layers)\=(?P<products>[^\s&]+))`)
	rxp_tile = regexp.MustCompile(`/swarmweb/tile/(?P<product>[^\/]+)/[^\/]+/(?:(?P<z>\d+)/(?P<x>\d+)/(?P<y>\d+)\.)?(?P<bing>\d+\.)?`)
	rxp_validframes = regexp.MustCompile(`/swarmweb/valid_frames\??(?:(?:products?=(?P<products>[^\s&]+)&?)|(?:[^\s&]+&?))*`)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			time.Sleep(2 * time.Second)
			break
		}
		go ParseLine(line, db, c)
	}
}

func ParseLine(line string, db *sql.DB, c chan amqp.Publishing) {
	match := rxp.FindStringSubmatch(line)
	if len(match) == 0 {
		log.Println("No regular expression found!")
		log.Println(line)
		return
	}
	
	var ip string
	if match[1] == "-" {
		ip = match[2]
	} else {
		ip = match[1]
	}
	geoip, err := GeoipLookup(db, ip)
	if err != nil {
		gp := GeoIP{Latitude: 0, Longitude: 0}
		geoip = &gp
	}
	var outstr []byte
	if conf.Swarm {
		var rtype, rprod string
		var rlat, rlon float64
		vf_match := rxp_validframes.FindStringSubmatch(match[5])
		ct_match := rxp_comptile.FindStringSubmatch(match[5])
		tile_match := rxp_tile.FindStringSubmatch(match[5])
		switch {
		case len(vf_match) > 0:
			rtype = "frames"
			rprod = vf_match[1]
		case len(ct_match) > 0:
			rtype = "comptile"
			rprod = ct_match[5]
			var z, x, y float64
			if len(ct_match[4]) > 0 {
				z, x, y = bing2zxy(ct_match[4])
			} else {
				z, err = strconv.ParseFloat(ct_match[1], 64)
				if err != nil {
					break
				}
				x, err = strconv.ParseFloat(ct_match[2], 64)
				if err != nil {
					break
				}
				y, err = strconv.ParseFloat(ct_match[3], 64)
				if err != nil {
					break
				}
			}
			rlon = tile2lon(x, z)
			rlat = tile2lat(y, z)
		case len(tile_match) > 0:
			rtype = "tile"
			rprod = tile_match[1]
			var z, x, y float64
			if len(tile_match[5]) > 0 {
				z, x, y = bing2zxy(tile_match[5])
			} else {
				z, err = strconv.ParseFloat(tile_match[2], 64)
				if err != nil {
					break
				}
				x, err = strconv.ParseFloat(tile_match[3], 64)
				if err != nil {
					break
				}
				y, err = strconv.ParseFloat(tile_match[4], 64)
				if err != nil {
					break
				}
			}
			rlon = tile2lon(x, z)
			rlat = tile2lat(y, z)

		}
		mm := Swarm_Message {
			User_ip: string(match[1]), 
			User_host: string(match[2]), 
			Lat: float64(geoip.Latitude), 
			Lon: float64(geoip.Longitude), 
			Timestamp: string(match[3]), 
			Method: string(match[4]), 
			Url: string(match[5]), 
			Response_code: string(match[6]), 
			Response_bytes: string(match[7]),
			Response_time: string(match[8]),
			Referer: string(match[9]),
			User_agent: string(match[10]),
			Request_type: rtype,
			Products: rprod,
			Request_lat: rlat,
			Request_lon: rlon,
		}
		outstr, err = json.Marshal(mm)
		if err != nil {
			log.Println("Error converting to JSON output")
			return	
		}
	} else {
		mm := Message{
			User_ip: string(match[1]), 
			User_host: string(match[2]), 
			Lat: float64(geoip.Latitude), 
			Lon: float64(geoip.Longitude), 
			Timestamp: string(match[3]), 
			Method: string(match[4]), 
			Url: string(match[5]), 
			Response_code: string(match[6]), 
			Response_bytes: string(match[7]),
			Response_time: string(match[8]),
			Referer: string(match[9]),
			User_agent: string(match[10]),
		}
		outstr, err = json.Marshal(mm)
		if err != nil {
			log.Println("Error converting to JSON output")
			return	
		}
	}
	msg := amqp.Publishing{
		DeliveryMode:	amqp.Persistent,
		Timestamp: 	time.Now(),
		ContentType: 	"text/plain",
		Body: 		[]byte(outstr),
	}
	if len(c) < cap(c) {
		c <- msg
	} else {
		log.Println("Channel Buffer is full!")
	}
}

func worker(c chan amqp.Publishing, url string, exchange string) {
	e, conn, ch := setup_amqp(url, exchange)
	for {
		select {
		case msg := <-c:
			err := ch.Publish(exchange, "", false, false, msg)
			if err != nil {
				log.Println("Publish Error:", err.Error())
				conn.Close()
			}
		case <-e:
			e, conn, ch = setup_amqp(url, exchange)
		}
	}
}

func tile2lon(x float64, z float64) float64 {
	r := x / math.Pow(2, z) * 360.0 - 180
	return r
}

func tile2lat(y float64, z float64) float64 {
	n := math.Pi - 2 * math.Pi * y / math.Pow(2, z)
	r := 180 / math.Pi * math.Atan(0.5 * (math.Exp(n) - math.Exp(-n)))
	return r
}

func bing2zxy(qkey string)(float64, float64, float64) {
	tileX := uint(0)
	tileY := uint(0)
	levelofdetail := len(qkey)
	for i := levelofdetail; i > 0; i-- {
		mask := uint(1) << uint(i - 1)
		switch qkey[levelofdetail - i] {
		case '0':
			break
		case '1':
			tileX |= mask
		case '2':
			tileY |= mask
		case '3':
			tileX |= mask
			tileY |= mask
		}
	}
	return float64(levelofdetail), float64(tileX), float64(tileY)
}

func setup_amqp(url, exchange string) (chan *amqp.Error, *amqp.Connection, *amqp.Channel) {
	e := make(chan *amqp.Error)
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Println("Bad Connection: Panic")
		panic(err)
	}
	conn.NotifyClose(e)
	ch, err := conn.Channel()
	if err != nil {
		log.Println("Bad Channel: Panic")
		panic(err)
	}
	err = ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Println("Bad Exchange: Panic")
		panic(err)
	}
	return e, conn, ch
}

const query = `SELECT
  city_location.country_code,
  country_blocks.country_name,
  city_location.region_code,
  region_names.region_name,
  city_location.city_name,
  city_location.postal_code,
  city_location.latitude,
  city_location.longitude,
  city_location.metro_code,
  city_location.area_code
FROM city_blocks
  NATURAL JOIN city_location
  INNER JOIN country_blocks ON
    city_location.country_code = country_blocks.country_code
  LEFT OUTER JOIN region_names ON
    city_location.country_code = region_names.country_code
    AND
    city_location.region_code = region_names.region_code
WHERE city_blocks.ip_start <= ?
ORDER BY city_blocks.ip_start DESC LIMIT 1`

func GeoipLookup(db *sql.DB, ip string) (*GeoIP, error) {
	IP := net.ParseIP(ip)
	reserved := false
	for _, net := range reservedIPs {
		if net.Contains(IP) {
			reserved = true
			break
		}
	}
	geoip := GeoIP{Ip: ip}
	if reserved {
		geoip.CountryCode = "RD"
		geoip.CountryName = "Reserved"
	} else {
		stmt, err := db.Prepare(query)
		if err != nil {
			if conf.Debug {
				log.Println("[debug] SQLite", err.Error())
			}
			return nil, err
		}
		defer stmt.Close()
		var uintIP uint32
		b := bytes.NewBuffer(IP.To4())
		binary.Read(b, binary.BigEndian, &uintIP)
		err = stmt.QueryRow(uintIP).Scan(
			&geoip.CountryCode,
			&geoip.CountryName,
			&geoip.RegionCode,
			&geoip.RegionName,
			&geoip.CityName,
			&geoip.ZipCode,
			&geoip.Latitude,
			&geoip.Longitude,
			&geoip.MetroCode,
			&geoip.AreaCode)
		if err != nil {
			return nil, err
		}
	}
	return &geoip, nil
}

type GeoIP struct {
	XMLName     xml.Name `json:"-" xml:"Response"`
	Ip          string   `json:"ip"`
	CountryCode string   `json:"country_code"`
	CountryName string   `json:"country_name"`
	RegionCode  string   `json:"region_code"`
	RegionName  string   `json:"region_name"`
	CityName    string   `json:"city" xml:"City"`
	ZipCode     string   `json:"zipcode"`
	Latitude    float32  `json:"latitude"`
	Longitude   float32  `json:"longitude"`
	MetroCode   string   `json:"metro_code"`
	AreaCode    string   `json:"areacode"`
}

// http://en.wikipedia.org/wiki/Reserved_IP_addresses
var reservedIPs = []net.IPNet{
	{net.IPv4(0, 0, 0, 0), net.IPv4Mask(255, 0, 0, 0)},
	{net.IPv4(10, 0, 0, 0), net.IPv4Mask(255, 0, 0, 0)},
	{net.IPv4(100, 64, 0, 0), net.IPv4Mask(255, 192, 0, 0)},
	{net.IPv4(127, 0, 0, 0), net.IPv4Mask(255, 0, 0, 0)},
	{net.IPv4(169, 254, 0, 0), net.IPv4Mask(255, 255, 0, 0)},
	{net.IPv4(172, 16, 0, 0), net.IPv4Mask(255, 240, 0, 0)},
	{net.IPv4(192, 0, 0, 0), net.IPv4Mask(255, 255, 255, 248)},
	{net.IPv4(192, 0, 2, 0), net.IPv4Mask(255, 255, 255, 0)},
	{net.IPv4(192, 88, 99, 0), net.IPv4Mask(255, 255, 255, 0)},
	{net.IPv4(192, 168, 0, 0), net.IPv4Mask(255, 255, 0, 0)},
	{net.IPv4(198, 18, 0, 0), net.IPv4Mask(255, 254, 0, 0)},
	{net.IPv4(198, 51, 100, 0), net.IPv4Mask(255, 255, 255, 0)},
	{net.IPv4(203, 0, 113, 0), net.IPv4Mask(255, 255, 255, 0)},
	{net.IPv4(224, 0, 0, 0), net.IPv4Mask(240, 0, 0, 0)},
	{net.IPv4(240, 0, 0, 0), net.IPv4Mask(240, 0, 0, 0)},
	{net.IPv4(255, 255, 255, 255), net.IPv4Mask(255, 255, 255, 255)},
}
