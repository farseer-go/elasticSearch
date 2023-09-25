package elasticSearch

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/types"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type ESContext struct {
	esConfig *esConfig
}

// initConfig 初始化配置文件
func initConfig(esName string) *ESContext {
	configString := configure.GetString("ElasticSearch." + esName)
	if configString == "" {
		panic("[farseer.yaml]找不到相应的配置：ElasticSearch." + esName)
	}
	esConfig := configure.ParseString[esConfig](configString)
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
		//分片
		var shardsCount int
		//复制
		var replicasCount int
		//刷新时间
		var refreshInterval int
		data := contextValueOf.Type().Field(i).Tag.Get("es")
		array := strings.Split(data, ";")
		for _, s := range array {
			if strings.HasPrefix(s, "index=") {
				indexName = s[len("index="):]
				indexName = ReplaceTime(indexName)
			}
			if strings.HasPrefix(s, "alias=") {
				aliasesName = s[len("alias="):]
				aliasesName = ReplaceTime(aliasesName)
			}
			if strings.HasPrefix(s, "shards=") {
				shardsCount, _ = strconv.Atoi(s[len("shards="):])
			}
			if strings.HasPrefix(s, "replicas=") {
				replicasCount, _ = strconv.Atoi(s[len("replicas="):])
			}
			if strings.HasPrefix(s, "refresh=") {
				refreshInterval, _ = strconv.Atoi(s[len("refresh="):])
			}
		}
		if indexName == "" {
			continue
		}
		if aliasesName == "" {
			aliasesName = indexName
		}
		// 再取IndexSet的子属性，并设置值
		field.Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(dbConfig), reflect.ValueOf(indexName), reflect.ValueOf(aliasesName), reflect.ValueOf(shardsCount), reflect.ValueOf(replicasCount), reflect.ValueOf(refreshInterval)})

	}
}

// ReplaceTime 替换索引内的时间
func ReplaceTime(index string) string {
	now := time.Now().String()
	index = strings.ReplaceAll(index, "yyyy", timeFormat(now, "yyyy"))
	index = strings.ReplaceAll(index, "MM", timeFormat(now, "MM"))
	index = strings.ReplaceAll(index, "dd", timeFormat(now, "dd"))
	index = strings.ReplaceAll(index, "hh", timeFormat(now, "hh"))
	index = strings.ReplaceAll(index, "mm", timeFormat(now, "mm"))
	return index
}

// 2022-09-07 21:41:30.9100946 +0800 CST m=+0.012157601
func timeFormat(time string, format string) string {
	array := strings.Split(time, " ")
	ymd := strings.Split(array[0], "-")
	hms := strings.Split(array[1], ":")
	switch format {
	case "yyyy":
		return ymd[0]
	case "MM":
		return ymd[1]
	case "dd":
		return ymd[2]
	case "hh":
		return hms[0]
	case "mm":
		return hms[1]
	}
	return ""
}

// GetEsType 获取elasticsearch对应类型
func GetEsType(val reflect.Type) string {
	typeStr := val.String()
	typeKind := val.Kind()
	if strings.HasSuffix(typeStr, ".Enum") {
		return "byte"
	}
	switch typeStr {
	case "time.Time":
		return "date"
	case "datetime.DateTime":
		return "date"
	}
	switch typeKind {
	case reflect.String:
		return "Keywords"
	case reflect.Bool:
		return "boolean"
	case reflect.Int:
		return "integer"
	case reflect.Int16:
		return "integer"
	case reflect.Int32:
		return "integer"
	case reflect.Int64:
		return "long"
	case reflect.Uint:
		return "integer"
	case reflect.Uint16:
		return "integer"
	case reflect.Uint32:
		return "integer"
	case reflect.Uint64:
		return "long"
	case reflect.Float32:
		return "double"
	case reflect.Float64:
		return "double"
	case reflect.Struct:
		return "object"
	case reflect.Array:
		return "object"
	case reflect.Slice:
		return "object"
	case reflect.Map:
		return "flattened"
	}
	return "byte"
}
