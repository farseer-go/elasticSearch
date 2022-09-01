package elasticSearch

import (
	"encoding/json"
	"github.com/farseer-go/collections"
	"github.com/olivere/elastic/v7"
	"reflect"
	"strconv"
	"strings"
)

// IndexSet 表操作
type IndexSet[Table any] struct {
	esContext *ESContext
	indexName string
	es        *elastic.Client
	esService *elastic.SearchService
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
	indexSet.data().CreateIndex(indexName).Do(ctx)
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
	//连接服务测试
	_, _, err := indexSet.es.Ping(indexSet.esContext.esConfig.Server).Do(ctx)
	if err != nil {
		panic(err)
	}
	return indexSet.es
}

// Select 筛选字段
func (indexSet *IndexSet[Table]) Select(fields ...string) *IndexSet[Table] {
	indexSet.esService = indexSet.data().Search().Index(indexSet.indexName).DocvalueFields(fields...)
	return indexSet
}

// Asc 正序排序
func (indexSet *IndexSet[Table]) Asc(field string) *IndexSet[Table] {
	indexSet.esService = indexSet.data().Search().Index(indexSet.indexName).Sort(field, true)
	return indexSet
}

// Desc 倒序排序
func (indexSet *IndexSet[Table]) Desc(field string) *IndexSet[Table] {
	indexSet.esService = indexSet.data().Search().Index(indexSet.indexName).Sort(field, false)
	return indexSet
}

// Del 删除指定Id数据
func (indexSet *IndexSet[Table]) Del(id string) error {
	_, err := indexSet.data().Delete().Index(indexSet.indexName).Id(id).Do(ctx)
	return err
}

// Where 倒序排序
func (indexSet *IndexSet[Table]) Where(field string, fieldValue string) *IndexSet[Table] {
	termQuery := elastic.NewTermQuery(field, fieldValue)
	indexSet.esService = indexSet.data().Search().Index(indexSet.indexName).Query(termQuery)
	return indexSet
}

// Insert 插入数据
func (indexSet *IndexSet[Table]) Insert(po Table) error {
	var err error
	poValueOf := reflect.ValueOf(po)
	Id := "0"
	for i := 0; i < poValueOf.NumField(); i++ {
		data := poValueOf.Type().Field(i).Tag.Get("gorm")
		if strings.HasPrefix(data, "primaryKey") {
			val := poValueOf.Field(i).Int()
			Id = strconv.FormatInt(val, 10)
			break
		}
	}
	_, err = indexSet.data().Index().Index(indexSet.indexName).Id(Id).BodyJson(po).Do(ctx)
	return err
}

// InsertArray 数组的形式插入
func (indexSet *IndexSet[Table]) InsertArray(array []Table) error {
	var err error
	for _, table := range array {
		poValueOf := reflect.ValueOf(table)
		Id := "0"
		for i := 0; i < poValueOf.NumField(); i++ {
			data := poValueOf.Type().Field(i).Tag.Get("gorm")
			if strings.HasPrefix(data, "primaryKey") {
				val := poValueOf.Field(i).Int()
				Id = strconv.FormatInt(val, 10)
				break
			}
		}
		_, err = indexSet.data().Index().Index(indexSet.indexName).Id(Id).BodyJson(table).Do(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertList 插入列表形式
func (indexSet *IndexSet[Table]) InsertList(list collections.List[Table]) error {
	var err error
	for i := 0; i < list.Count(); i++ {
		poValueOf := reflect.ValueOf(list.Index(i))
		Id := "0"
		for i := 0; i < poValueOf.NumField(); i++ {
			data := poValueOf.Type().Field(i).Tag.Get("gorm")
			if strings.HasPrefix(data, "primaryKey") {
				val := poValueOf.Field(i).Int()
				Id = strconv.FormatInt(val, 10)
				break
			}
		}
		_, err = indexSet.data().Index().Index(indexSet.indexName).Id(Id).BodyJson(list.Index(i)).Do(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// ToList 转换List集合
func (indexSet *IndexSet[Table]) ToList() collections.List[Table] {
	if indexSet.esService == nil {
		indexSet.esService = indexSet.data().Search().Index(indexSet.indexName)
	}
	resp, _ := indexSet.esService.TrackTotalHits(true).Do(ctx)
	hitArray := resp.Hits.Hits
	var lst []Table
	for _, hit := range hitArray {
		var entity Table
		_ = json.Unmarshal(hit.Source, &entity)
		//添加元素
		lst = append(lst, entity)
	}
	return collections.NewList[Table](lst...)
}

// ToPageList 转成分页集合
func (indexSet *IndexSet[Table]) ToPageList(pageSize int, pageIndex int) collections.List[Table] {
	if indexSet.esService == nil {
		indexSet.esService = indexSet.data().Search().Index(indexSet.indexName)
	}
	resp, _ := indexSet.esService.From((pageIndex - 1) * pageSize).Size(pageSize).Pretty(true).Do(ctx)
	hitArray := resp.Hits.Hits
	var lst []Table
	for _, hit := range hitArray {
		var entity Table
		_ = json.Unmarshal(hit.Source, &entity)
		//添加元素
		lst = append(lst, entity)
	}
	return collections.NewList[Table](lst...)
}
