package elasticSearch

import (
	"github.com/farseer-go/fs/configure"
	"reflect"
	"strings"
)

type ESContext struct {
	esConfig *elasticConfig
}

// initConfig 初始化配置文件
func initConfig(esName string) *ESContext {
	configString := configure.GetString("ElasticSearch." + esName)
	if configString == "" {
		panic("[farseer.yaml]找不到相应的配置：ElasticSearch." + esName)
	}
	esConfig := configure.ParseConfig[elasticConfig](configString)
	esContext := &ESContext{
		esConfig: &esConfig,
	}
	return esContext
}

// NewContext 数据库上下文初始化
// esName：数据库配置名称
func NewContext[TEsContext any](esName string) *TEsContext {
	if esName == "" {
		panic("esName入参必须设置有效的值")
	}
	dbConfig := initConfig(esName) // 嵌入类型
	//var dbName string       // 数据库配置名称
	customContext := new(TEsContext)
	contextValueOf := reflect.ValueOf(customContext).Elem()

	for i := 0; i < contextValueOf.NumField(); i++ {
		field := contextValueOf.Field(i)
		fieldType := field.Type().String()
		if !field.CanSet() || !strings.HasPrefix(fieldType, "data.TableSet[") {
			continue
		}
		data := contextValueOf.Type().Field(i).Tag.Get("data")
		var tableName string
		if strings.HasPrefix(data, "name=") {
			tableName = data[len("name="):]
		}
		if tableName == "" {
			continue
		}
		// 再取tableSet的子属性，并设置值
		field.Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(dbConfig), reflect.ValueOf(tableName)})
	}
	return customContext
}
