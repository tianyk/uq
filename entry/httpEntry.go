package entry

import (
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
)

const (
	queuePrefixV1 = "/v1/queues" // HTTP REST API
	adminPrefixV1 = "/v1/admin"  // HTTP ADMIN API
)

type HttpEntry struct {
	host         string
	port         int
	adminMux     map[string]func(http.ResponseWriter, *http.Request, string) // 路由
	server       *http.Server                                                // HttpServer
	stopListener *StopListener
	messageQueue queue.MessageQueue // 消息队列
}

/**
 * 创建一个HTTP协议服务
 * @param {[type]} host         string              [description]
 * @param {[type]} port         int                 [description]
 * @param {[type]} messageQueue queue.MessageQueue) (*HttpEntry,  error [description]
 */
func NewHttpEntry(host string, port int, messageQueue queue.MessageQueue) (*HttpEntry, error) {
	h := new(HttpEntry)

	// key String Value Function
	h.adminMux = map[string]func(http.ResponseWriter, *http.Request, string){
		"/stat":  h.statHandler,  // 查看topic、line状态
		"/empty": h.emptyHandler, // 清空topic、line状态
		"/rm":    h.rmHandler,    // 删除一个topic、line
	}

	// http://studygolang.com/static/pkgdoc/pkg/net_http.htm
	// http包提供了HTTP客户端和服务端的实现。
	// 创建一个Server
	addr := Addrcat(host, port)
	server := new(http.Server)
	server.Addr = addr
	server.Handler = h

	h.host = host
	h.port = port
	h.server = server
	h.messageQueue = messageQueue

	return h, nil
}

/**
 * 解析出来路由，解析key
 * @param  {[type]} h *HttpEntry)   ServeHTTP(w http.ResponseWriter, req *http.Request [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !AllowMethod(w, req.Method, "HEAD", "GET", "POST", "PUT", "DELETE") {
		return
	}

	// queuePrefixV1 /v1/queues
	if strings.HasPrefix(req.URL.Path, queuePrefixV1) {
		// 截取api定义的作为参数
		// e.g. /v1/queues/foo key is foo
		key := req.URL.Path[len(queuePrefixV1):]
		h.queueHandler(w, req, key)
		return
	} else if strings.HasPrefix(req.URL.Path, adminPrefixV1) {
		// /v1/admin
		// e.g. /v1/admin/stat/foo key is stat/foo
		key := req.URL.Path[len(adminPrefixV1):]
		h.adminHandler(w, req, key)
		return
	}

	http.Error(w, "404 Not Found!", http.StatusNotFound)
	return
}

/**
 * 消息队列的HTTP handler
 * 主要包括
 * add
 * push
 * pop
 * del
 * @param  {[type]} h *HttpEntry)   queueHandler(w http.ResponseWriter, req *http.Request, key string [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) queueHandler(w http.ResponseWriter, req *http.Request, key string) {
	switch req.Method {
	case "PUT":
		h.addHandler(w, req, key)
	case "POST":
		h.pushHandler(w, req, key)
	case "GET":
		h.popHandler(w, req, key)
	case "DELETE":
		h.delHandler(w, req, key)
	default:
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
	}
	return
}

/**
 * Admin Server Handler
 * 需要解析两次key,第一次解析出来时stat/empty，第二次解析出来topic、line
 * @param  {[type]} h *HttpEntry)   adminHandler(w http.ResponseWriter, req *http.Request, key string [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) adminHandler(w http.ResponseWriter, req *http.Request, key string) {
	for prefix, handler := range h.adminMux {
		if strings.HasPrefix(key, prefix) {
			key = key[len(prefix):]
			handler(w, req, key)
			return
		}
	}

	http.Error(w, "404 Not Found!", http.StatusNotFound)
	return
}

func writeErrorHttp(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *Error:
		e.WriteTo(w)
	default:
		// log.Printf("unexpected error: %v", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
	}
}

/**
 * 添加一个topic或者line
 * @param  {[type]} h *HttpEntry)   addHandler(w http.ResponseWriter, req *http.Request, key string [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) addHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := req.ParseForm()
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrInternalError,
			err.Error(),
		))
		return
	}

	topicName := req.FormValue("topic")
	lineName := req.FormValue("line")
	key = topicName + "/" + lineName // key = topic + line
	recycle := req.FormValue("recycle")

	// log.Printf("creating... %s %s", key, recycle)
	err = h.messageQueue.Create(key, recycle)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

/**
 * push数据到topic
 * @param  {[type]} h *HttpEntry)   pushHandler(w http.ResponseWriter, req *http.Request, key string [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) pushHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := req.ParseForm()
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrInternalError,
			err.Error(),
		))
		return
	}

	data := []byte(req.FormValue("value"))
	err = h.messageQueue.Push(key, data)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) popHandler(w http.ResponseWriter, req *http.Request, key string) {
	id, data, err := h.messageQueue.Pop(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-UQ-ID", id)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

//
func (h *HttpEntry) delHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := h.messageQueue.Confirm(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Admin Server API
/**
 * 获取一个topic、line的状态
 * @param  {[type]} h *HttpEntry)   statHandler(w http.ResponseWriter, req *http.Request, key string [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) statHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "GET" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	qs, err := h.messageQueue.Stat(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}

	log.Printf("qs: %v", qs)
	data, err := qs.ToJson()
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrInternalError,
			err.Error(),
		))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

/**
 * 清空一个topic、line
 * @param  {[type]} h *HttpEntry)   emptyHandler(w http.ResponseWriter, req *http.Request, key string [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) emptyHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "DELETE" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	err := h.messageQueue.Empty(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

/**
 * 删除topic、line
 * @param  {[type]} h *HttpEntry)   rmHandler(w http.ResponseWriter, req *http.Request, key string [description]
 * @return {[type]}   [description]
 */
func (h *HttpEntry) rmHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "DELETE" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	err := h.messageQueue.Remove(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	// http state 204
	// http://www.netingcn.com/http-status-204.html
	w.WriteHeader(http.StatusNoContent)
}

/**
 * 启动一个Server
 * 1. 开启一个tcp服务，监听Stop指令
 */
func (h *HttpEntry) ListenAndServe() error {
	// 监听停止指令
	addr := Addrcat(h.host, h.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := NewStopListener(l)
	if err != nil {
		return err
	}
	h.stopListener = stopListener

	log.Printf("http entrance serving at %s...", addr)
	// http://studygolang.com/static/pkgdoc/pkg/net_http.htm#Server.Serve
	// Serve会接手监听器l收到的每一个连接，并为每一个连接创建一个新的服务go程。该go程会读取请求，然后调用srv.Handler回复请求。
	return h.server.Serve(h.stopListener)
}

func (h *HttpEntry) Stop() {
	log.Printf("http entry stoping...")
	h.stopListener.Stop()
	h.messageQueue.Close()
}
