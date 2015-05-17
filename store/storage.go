package store

const (
	ErrNotExisted     string = "Data Not Existed"         // 数据不存在
	ErrModeNotMatched string = "Storage Mode Not Matched" // 存储类型不支持
)

// Storage接口提供四个方法，Set、Get、Del、Close
type Storage interface {
	Set(key string, data []byte) error
	Get(key string) ([]byte, error)
	Del(key string) error
	Close() error
}
