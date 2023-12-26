package test

import (
	"fmt"
	"github.com/farseer-go/elasticSearch"
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/flog"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type TestEsContext struct {
	User elasticSearch.IndexSet[UserPO] `es:"index=user_yyyy_MM_dd;alias=user_yyyy_MM_dd;shards=2;replicas=2;refresh=2"`
}

type UserPO struct {
	Id int `es:"primaryKey" es_type:"integer"`
	// 用户名称
	Name string `es_type:"keyword"`
	// 用户年龄
	Age int `es_type:"integer"`
	//公司
	Company string
}

func TestNewContext(t *testing.T) {
	// 设置配置默认值，模拟配置文件
	configure.SetDefault("ElasticSearch.test", "Server=http://es:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := elasticSearch.NewContext[TestEsContext]("test")
	context.User.SetIndexName("user_index_01", "user_index_alis_01")
	assert.Equal(t, "user_index_01", context.User.GetIndexName())
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
	elasticSearch.InitContext(&context, "test")
	context.User.SetIndexName("user_index_01", "user_index_alis_01")
	assert.Equal(t, "user_index_01", context.User.GetIndexName())

}
