package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/buaazp/uq/admin"
	"github.com/buaazp/uq/entry"
	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
	. "github.com/buaazp/uq/utils"
)

var (
	ip        string
	host      string
	port      int
	adminPort int
	pprofPort int
	protocol  string
	db        string
	dir       string
	logFile   string
	etcd      string
	cluster   string
)

// init()会在main()函数之前执行
func init() {
	// http://blog.studygolang.com/2013/02/%E6%A0%87%E5%87%86%E5%BA%93-%E5%91%BD%E4%BB%A4%E8%A1%8C%E5%8F%82%E6%95%B0%E8%A7%A3%E6%9E%90flag/
	// flag.XxxVar()，将flag绑定到一个变量上
	// 赋值-ip=127.0.0.1 -host=192.168.0.1
	// 使用-h or -help 查看说明
	flag.StringVar(&ip, "ip", "127.0.0.1", "self ip/host address")
	flag.StringVar(&host, "host", "0.0.0.0", "listen ip")
	flag.IntVar(&port, "port", 8808, "listen port")
	flag.IntVar(&adminPort, "admin-port", 8809, "admin listen port") // 管理
	flag.IntVar(&pprofPort, "pprof-port", 8080, "pprof listen port")
	flag.StringVar(&protocol, "protocol", "redis", "frontend interface type [redis/mc/http]") // 前端访问协议
	flag.StringVar(&db, "db", "goleveldb", "backend storage type [goleveldb/memdb]")          // 后台存储类型
	flag.StringVar(&dir, "dir", "./data", "backend storage path")                             // 存储路径
	flag.StringVar(&logFile, "log", "", "uq log path")                                        // uq日志文件路径，相对于dir目录
	flag.StringVar(&etcd, "etcd", "", "etcd service location")                                // etct
	flag.StringVar(&cluster, "cluster", "uq", "cluster name in etcd")
}

// 校验是否在列表中
func belong(single string, team []string) bool {
	for _, one := range team {
		if single == one {
			return true
		}
	}
	return false
}

// 校验参数是否支持
// 校验db及protocol
func checkArgs() bool {
	if !belong(db, []string{"goleveldb", "memdb"}) {
		fmt.Printf("db mode %s is not supported!\n", db)
		return false
	}
	if !belong(protocol, []string{"redis", "mc", "http"}) {
		fmt.Printf("protocol %s is not supported!\n", protocol)
		return false
	}
	return true
}

func main() {
	// 开启多核支持
	runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() {
		fmt.Printf("byebye! uq see u later! 😄\n")
	}()

	// 解析命令行参数
	flag.Parse()

	// 校验参数
	if !checkArgs() {
		return
	}

// 创建存储目录 dir参数
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		fmt.Printf("mkdir %s error: %s\n", dir, err)
		return
	}
	if logFile == "" {
		// 连接路径 支持多个参数，支持..
		logFile = path.Join(dir, "uq.log")
	}
	// http://studygolang.com/static/pkgdoc/pkg/os.htm#pkg-constants
	// 多种方式组合 不存在创建、只写、Append -rw-r--r--
	logf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("log open error: %s\n", err)
		return
	}

	// http://studygolang.com/static/pkgdoc/pkg/log.htm#pkg-constants
	// SetFlags设置标准logger的输出选项。
	// 文件无路径名+行号：d.go:23（会覆盖掉Llongfile） | 标准logger的初始值 | 微秒分辨率：01:23:23.123123（用于增强Ltime位）
	// e.g. [uq] 2015/05/17 21:47:09.294358 test.go:36: You name doog, you age 17
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	// SSetPrefix设置logger的输出前缀。
	log.SetPrefix("[uq] ")
	// SetOutput设置标准logger的输出目的地，默认是标准错误输出。
	log.SetOutput(logf)

	fmt.Printf("uq started! 😄\n")

	// 存储方式
	var storage store.Storage
	// if db == "rocksdb" {
	// 	dbpath := path.Clean(path.Join(dir, "uq.db"))
	// 	log.Printf("dbpath: %s", dbpath)
	// 	storage, err = store.NewRockStore(dbpath)
	// } else if db == "goleveldb" {

	// Go语言实现的 LevelDB key/value 数据库 - goleveldb
	// https://github.com/syndtr/goleveldb.git
	if db == "goleveldb" {
		// Clean函数通过单纯的词法操作返回和path代表同一地址的最短路径。
		// http://studygolang.com/static/pkgdoc/pkg/path.htm#Clean
		dbpath := path.Clean(path.Join(dir, "uq.db"))
		log.Printf("dbpath: %s", dbpath)
		storage, err = store.NewLevelStore(dbpath)
	} else if db == "memdb" {
		storage, err = store.NewMemStore()
	} else {
		fmt.Printf("store %s is not supported!\n", db)
		return
	}
	if err != nil {
		fmt.Printf("store init error: %s\n", err)
		return
	}

	// 处理etcdServer配置
	// http://www.infoq.com/cn/news/2014/07/etcd-cluster-discovery
	// http://www.infoq.com/cn/articles/coreos-analyse-etcd
	// https://github.com/coreos/etcd.git
	var etcdServers []string
	if etcd != "" {
		// http://studygolang.com/static/pkgdoc/pkg/strings.htm
		// strings包实现了用于操作字符的简单函数.
		etcdServers = strings.Split(etcd, ",")
		for i, etcdServer := range etcdServers {
			// 判断s是否有前缀字符串prefix
			if !strings.HasPrefix(etcdServer, "http://") {
				etcdServers[i] = "http://" + etcdServer
			}
		}
	}

	// 消息队列
	var messageQueue queue.MessageQueue
	// messageQueue, err = queue.NewFakeQueue(storage, ip, port, etcdServers, cluster)
	// if err != nil {
	// 	fmt.Printf("queue init error: %s\n", err)
	// 	storage.Close()
	// 	return
	// }
	messageQueue, err = queue.NewUnitedQueue(storage, ip, port, etcdServers, cluster)
	if err != nil {
		fmt.Printf("queue init error: %s\n", err)
		storage.Close()
		return
	}

	var entrance entry.Entrance
	if protocol == "http" {
		entrance, err = entry.NewHttpEntry(host, port, messageQueue)
	} else if protocol == "mc" {
		entrance, err = entry.NewMcEntry(host, port, messageQueue)
	} else if protocol == "redis" {
		entrance, err = entry.NewRedisEntry(host, port, messageQueue)
	} else {
		fmt.Printf("protocol %s is not supported!\n", protocol)
		return
	}
	if err != nil {
		fmt.Printf("entry init error: %s\n", err)
		messageQueue.Close()
		return
	}

	stop := make(chan os.Signal)
	entryFailed := make(chan bool)
	adminFailed := make(chan bool)
	signal.Notify(stop, syscall.SIGINT, os.Interrupt, os.Kill)
	var wg sync.WaitGroup

	// start entrance server
	go func(c chan bool) {
		wg.Add(1)
		defer wg.Done()
		err := entrance.ListenAndServe()
		if err != nil {
			if !strings.Contains(err.Error(), "stopped") {
				fmt.Printf("entry listen error: %s\n", err)
			}
			close(c)
		}
	}(entryFailed)

	var adminServer admin.AdminServer
	adminServer, err = admin.NewAdminServer(host, adminPort, messageQueue)
	if err != nil {
		fmt.Printf("admin init error: %s\n", err)
		entrance.Stop()
		return
	}

	// start admin server
	go func(c chan bool) {
		wg.Add(1)
		defer wg.Done()
		err := adminServer.ListenAndServe()
		if err != nil {
			if !strings.Contains(err.Error(), "stopped") {
				fmt.Printf("entry listen error: %s\n", err)
			}
			close(c)
		}
	}(adminFailed)

	// start pprof server
	go func() {
		addr := Addrcat(host, pprofPort)
		log.Println(http.ListenAndServe(addr, nil))
	}()

	select {
	case <-stop:
		// log.Printf("got signal: %v", signal)
		adminServer.Stop()
		log.Printf("admin server stoped.")
		entrance.Stop()
		log.Printf("entrance stoped.")
	case <-entryFailed:
		messageQueue.Close()
	case <-adminFailed:
		entrance.Stop()
	}
	wg.Wait()
}
