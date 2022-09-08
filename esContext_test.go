package elasticSearch

import (
	"fmt"
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/core/eumLogLevel"
	"github.com/farseer-go/fs/flog"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/type/datetime"
	"testing"
	"time"
)

type TestEsContext struct {
	User IndexSet[UserPO] `es:"index=user_yyyy_MM_dd_hh_mm;alias=user_01,user_02,user_03;shards=2;replicas=2;refresh=2"`
}

type UserPO struct {
	Id int `es:"primaryKey" es_type:"integer"`
	// 用户名称
	Name string `es_type:"keyword"`
	// 用户年龄
	Age       int `es_type:"integer"`
	Enum      eumLogLevel.Enum
	StructStr struct{}
	Array     []string
	Liststr   collections.List[UserPO]
	Int       int
	Int16     int16
	Int32     int32
	Int64     int64
	UInt      uint
	UInt16    uint16
	UInt32    uint32
	UInt64    uint64
	Float32   float32
	Float64   float64
	MapStr    map[string]interface{}
	BoolStr   bool
	DateStr   time.Time
	DateStr2  datetime.DateTime
}

func TestNewContext(t *testing.T) {
	// 设置配置默认值，模拟配置文件
	configure.SetDefault("ElasticSearch.test", "Server=http://es:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("test")

	assert.Equal(t, "user", context.User.indexName)
}

func TestInitContext(t *testing.T) {
	// 获取当前时间
	now := time.Now()
	flog.Println(now)
	// 序列化输出结果
	fmt.Printf("当前时间为:%v\n", now)
	// 通过包下提供的函数获取年\月\日\时\分\秒等信息
	year := now.Year()
	month := now.Month()
	day := now.Day()
	hour := now.Hour()
	minute := now.Minute()
	second := now.Second()

	// 序列化输出结果集
	fmt.Printf("%d-%02d-%02d %02d:%02d:%02d\n", year, month, day, hour, minute, second)
	flog.Println(year, month, day, hour, minute, second)
	// 设置配置默认值，模拟配置文件
	configure.SetDefault("ElasticSearch.test", "Server=http://es:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")

	var context TestEsContext
	InitContext(&context, "test")
	assert.Equal(t, "user", context.User.indexName)

	InitContext(&context, "test")
	assert.Equal(t, "user", context.User.indexName)

	context2 := new(TestEsContext)
	InitContext(context2, "test")
	assert.Equal(t, "user", context2.User.indexName)

	InitContext(context2, "test")
	assert.Equal(t, "user", context2.User.indexName)
}
