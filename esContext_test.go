package elasticSearch

import (
	"github.com/farseer-go/fs/configure"
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestEsContext struct {
	User IndexSet[UserPO] `es:"index=user&alias=user_01,user_02,user_03"`
}

type UserPO struct {
	Id int `es:"primaryKey" es_type:"long"`
	// 用户名称
	Name string `es_type:"keyword"`
	// 用户年龄
	Age int `es_type:"long"`
}

func TestNewContext(t *testing.T) {
	// 设置配置默认值，模拟配置文件
	configure.SetDefault("ElasticSearch.test", "Server=http://es:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("test")

	assert.Equal(t, "user", context.User.indexName)
}

func TestInitContext(t *testing.T) {
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
