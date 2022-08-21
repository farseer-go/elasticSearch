package elasticSearch

import (
	"encoding/json"
	"github.com/farseer-go/collections"
	"github.com/olivere/elastic/v7"
	"reflect"
)

// IndexSet 表操作
type IndexSet[Table any] struct {
	esContext *ESContext
	indexName string
	es        *elastic.Client
	err       error
}

// Init 在反射的时候会调用此方法
func (indexSet *IndexSet[Table]) Init(esContext *ESContext, indexName string) {
	indexSet.esContext = esContext
	indexSet.SetIndexName(indexName)
}

// SetIndexName 设置索引名称
func (indexSet *IndexSet[Table]) SetIndexName(indexName string) {
	indexSet.indexName = indexName
	if indexSet.es == nil {
		return
	}
	indexSet.es.Search().Index(indexSet.indexName)
}

// GetIndexName 获取索引名称
func (indexSet *IndexSet[Table]) GetIndexName() string {
	return indexSet.indexName
}

// 初始化ES
func (indexSet *IndexSet[Table]) data() *elastic.Client {
	if indexSet.es == nil {
		es, _ := elastic.NewClient(
			elastic.SetURL(indexSet.esContext.esConfig.Server),
			elastic.SetBasicAuth(indexSet.esContext.esConfig.Username, indexSet.esContext.esConfig.Password),
			elastic.SetSniff(false), //非集群下，关闭嗅探机制
		)
		indexSet.es = es
	}
	return indexSet.es
}

// Select 筛选字段
func (indexSet *IndexSet[Table]) Select(fields ...string) *IndexSet[Table] {
	indexSet.data().Search().Index(indexSet.indexName).DocvalueFields(fields...)
	return indexSet
}

// Asc 正序排序
func (indexSet *IndexSet[Table]) Asc(field string) *IndexSet[Table] {
	indexSet.data().Search().Index(indexSet.indexName).Sort(field, true)
	return indexSet
}

// Desc 倒序排序
func (indexSet *IndexSet[Table]) Desc(field string) *IndexSet[Table] {
	indexSet.data().Search().Index(indexSet.indexName).Sort(field, false)
	return indexSet
}

// Insert 插入数据
func (indexSet *IndexSet[Table]) Insert(po Table) (bool, error) {
	var putResp *elastic.PutMappingResponse
	var err error
	poMap := make(map[string]interface{})
	typeOfPo := reflect.TypeOf(po)
	valueOfPo := reflect.ValueOf(po)
	// 通过 #NumField 获取结构体字段的数量
	for i := 0; i < typeOfPo.NumField(); i++ {
		key := typeOfPo.Field(i).Name
		value := valueOfPo.Field(i)
		poMap[key] = value
	}
	putResp, err = indexSet.es.PutMapping().Index(indexSet.indexName).IgnoreUnavailable(true).BodyJson(poMap).Do(ctx)
	if err != nil {
		return false, err
	}
	if putResp.Acknowledged {
		return true, err
	}
	return false, err
}

// ToList 转换List集合
func (indexSet *IndexSet[Table]) ToList() collections.List[Table] {
	resp, _ := indexSet.data().Search().Index(indexSet.indexName).TrackTotalHits(true).Do(ctx)
	hitArray := resp.Hits.Hits
	var lst []Table
	for _, hit := range hitArray {
		var entity Table
		poMap := hit.Fields
		marshal, _ := json.Marshal(poMap)
		_ = json.Unmarshal(marshal, &entity)
		//添加元素
		lst = append(lst, entity)
	}
	return collections.NewList[Table](lst...)
}
