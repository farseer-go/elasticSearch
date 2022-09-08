package elasticSearch

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/core/eumLogLevel"
	"github.com/farseer-go/fs/flog"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

// 枚举
const (
	a int = iota // a = 0
	b int = iota // b = 1
	c int = iota // c = 2
)

func TestPOEsType(t *testing.T) {
	po := UserPO{Age: 20, Name: "小小", Id: 100, Enum: eumLogLevel.Debug}
	//表结构处理
	miTable := make(map[string]interface{}, 0)
	poValueOf := reflect.ValueOf(po)
	poTypeOf := reflect.TypeOf(po)
	for i := 0; i < poValueOf.NumField(); i++ {
		prop := poValueOf.Type().Field(i).Name
		esType := poValueOf.Type().Field(i).Tag.Get("es_type")
		poType := poTypeOf.Field(i).Type
		if esType != "" {
			miTable[prop] = esType // mi{"type": esType}
		} else {
			miTable[prop] = GetEsType(poType)
		}
	}
	flog.Println(miTable)
}

func TestIndexSet_Init(t *testing.T) {
	po := UserPO{Age: 20, Name: "小小", Id: 100}
	typeOfPo := reflect.TypeOf(po)
	valueOfPo := reflect.ValueOf(po)
	// 通过 #NumField 获取结构体字段的数量
	for i := 0; i < typeOfPo.NumField(); i++ {
		key := typeOfPo.Field(i).Name
		value := valueOfPo.Field(i)
		flog.Infof("key :%s value: %s", key, value)
	}
}

func TestIndexSet_Insert(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	po := UserPO{Name: "小强", Age: 10, Id: 12}
	err := context.User.Insert(po)
	assert.Equal(t, err, nil)
}

func TestIndexSet_InsertArray(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	var array []UserPO
	po := UserPO{Name: "小强2", Age: 10, Id: 1}
	array = append(array, po)
	err := context.User.InsertArray(array)
	assert.Equal(t, err, nil)
}

func TestIndexSet_InsertList(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	list := collections.NewList(UserPO{Name: "小丽", Age: 20, Id: 2}, UserPO{Name: "小王", Age: 30, Id: 3})
	err := context.User.InsertList(list)
	assert.Equal(t, err, nil)
}

func TestIndexSet_Select(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	list := context.User.Select("Name").ToList()
	assert.Equal(t, list.First().Name, "小强")
}
func TestIndexSet_Asc(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	list := context.User.Asc("Id").ToList()
	assert.Equal(t, list.First().Name, "小强2")
}

func TestIndexSet_Desc(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	list := context.User.Desc("Id").ToList()
	assert.Equal(t, list.First().Name, "小强")
}

func TestIndexSet_GetIndexName(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	name := context.User.GetIndexName()
	assert.Equal(t, name, "user")
}

func TestIndexSet_ToList(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	list := context.User.ToList()
	assert.Equal(t, list.Count(), 4)
}

func TestIndexSet_ToPageList(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	list := context.User.ToPageList(1, 2)
	assert.Equal(t, list.First().Name, "小强2")
}

func TestIndexSet_Where(t *testing.T) {
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	context := NewContext[TestEsContext]("log_es")
	list := context.User.Where("Age", "30").ToList()
	assert.Equal(t, list.First().Name, "小王")
}
