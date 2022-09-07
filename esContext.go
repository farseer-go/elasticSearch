package elasticSearch

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/types"
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
	var context TEsContext
	InitContext(&context, esName)
	return &context
}

// InitContext 数据库上下文初始化
// esName：数据库配置名称
func InitContext[TEsContext any](esContext *TEsContext, esName string) {
	if esName == "" {
		panic("esName入参必须设置有效的值")
	}
	dbConfig := initConfig(esName) // 嵌入类型
	//var dbName string       // 数据库配置名称
	contextValueOf := reflect.ValueOf(esContext).Elem()

	for i := 0; i < contextValueOf.NumField(); i++ {
		field := contextValueOf.Field(i)
		if !field.CanSet() {
			continue
		}

		_, isIndexSet := types.IsEsIndexSet(field)
		if !isIndexSet {
			continue
		}

		//表名
		var indexName string
		//别名
		var aliasesName string
		data := contextValueOf.Type().Field(i).Tag.Get("es")
		array := strings.Split(data, ";")
		for _, s := range array {
			if strings.HasPrefix(s, "index=") {
				indexName = s[len("index="):]
			}
			if strings.HasPrefix(s, "alias=") {
				aliasesName = s[len("alias="):]
			}
		}
		if indexName == "" {
			continue
		}
		if aliasesName == "" {
			aliasesName = indexName
		}
		// 再取IndexSet的子属性，并设置值
		field.Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(dbConfig), reflect.ValueOf(indexName), reflect.ValueOf(aliasesName)})

	}
}
