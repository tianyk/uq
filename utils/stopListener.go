package utils

import (
	"errors"
	"net"
	"time"
)

// 继承自net.TCPListener
type StopListener struct {
	*net.TCPListener          //Wrapped listener // 继承net.TCPListener
	stop             chan int //Channel used only to indicate listener should shutdown
}

var StoppedError = errors.New("Listener stopped")

func NewStopListener(l net.Listener) (*StopListener, error) {
	// 判断类型是不是TCPListener
	tcpL, ok := l.(*net.TCPListener)

	if !ok {
		return nil, errors.New("Cannot wrap listener")
	}

	retval := &StopListener{}
	retval.TCPListener = tcpL
	retval.stop = make(chan int) // 创建一个管道 标志

	return retval, nil
}

func (sl *StopListener) Accept() (net.Conn, error) {
	for {
		// 设置监听器执行的期限，t为Time零值则会关闭期限限制。超时时间
		//Wait up to one second for a new connection
		sl.SetDeadline(time.Now().Add(time.Second))

		// Accept用于实现Listener接口的Accept方法；他会等待下一个呼叫，并返回一个该呼叫的Conn接口。
		newConn, err := sl.TCPListener.Accept()

		//Check for the channel being closed
		select {
		case <-sl.stop:
			return nil, StoppedError
		default:
			//If the channel is still open, continue as normal
		}

		if err != nil {
			netErr, ok := err.(net.Error)

			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
		}

		return newConn, err
	}
}

func (sl *StopListener) Stop() {
	close(sl.stop)
}
