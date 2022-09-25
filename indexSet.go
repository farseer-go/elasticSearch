package elasticSearch

import (
	"encoding/json"
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/flog"
	"github.com/olivere/elastic/v7"
	"reflect"
	"strconv"
	"strings"
)

// IndexSet 表操作
type IndexSet[Table any] struct {
	esContext       *ESContext
	indexName       string
	aliasesName     string
	es              *elastic.Client
	esService       *elastic.SearchService
	err             error
	ShardsCount     int
	ReplicasCount   int
	RefreshInterval int
}
type mi map[string]interface{}

// Init 在反射的时候会调用此方法
func (indexSet *IndexSet[Table]) Init(esContext *ESContext, indexName string, indexAliases string, shardsCount int, replicasCount int, refreshInterval int) {
	indexSet.esContext = esContext
	indexSet.ShardsCount = shardsCount
	indexSet.ReplicasCount = replicasCount
	indexSet.RefreshInterval = refreshInterval
	indexSet.SetIndexName(indexName, indexAliases)
}

// SetIndexName 设置索引名称
func (indexSet *IndexSet[Table]) SetIndexName(indexName string, indexAliases string) {
	indexSet.indexName = indexName
	indexSet.aliasesName = indexAliases
}

// GetIndexName 获取索引名称
func (indexSet *IndexSet[Table]) GetIndexName() string {
	return indexSet.indexName
}

// SetAliasesName 设置别名
func (indexSet *IndexSet[Table]) SetAliasesName(aliasesName string) error {
	_, err := indexSet.data().Alias().Add(indexSet.indexName, aliasesName).Do(ctx)
	return err
}

// WhenNotExistsAddIndex 当不存在的时候创建索引
func (indexSet *IndexSet[Table]) WhenNotExistsAddIndex(po Table) {
	do, _ := indexSet.data().IndexExists(indexSet.indexName).Do(ctx)
	if !do {
		indexSet.CreateIndex(po)
	}
}

// CreateIndex 创建索引
func (indexSet *IndexSet[Table]) CreateIndex(po Table) {
	//表结构处理
	miTable := make(map[string]interface{})
	poValueOf := reflect.ValueOf(po)
	poTypeOf := reflect.TypeOf(po)
	for i := 0; i < poValueOf.NumField(); i++ {
		prop := poValueOf.Type().Field(i).Name
		esType := poValueOf.Type().Field(i).Tag.Get("es_type")
		poType := poTypeOf.Field(i).Type
		if esType != "" {
			miTable[prop] = mi{"type": esType}
		} else {
			miTable[prop] = mi{"type": GetEsType(poType)}
		}
	}
	_shardsCount := indexSet.esContext.esConfig.ShardsCount
	_replicasCount := indexSet.esContext.esConfig.ReplicasCount
	_refreshInterval := indexSet.esContext.esConfig.RefreshInterval
	if indexSet.ShardsCount > 0 {
		_shardsCount = indexSet.ShardsCount
	}
	if indexSet.ReplicasCount > 0 {
		_replicasCount = indexSet.ReplicasCount
	}
	if indexSet.RefreshInterval > 0 {
		_refreshInterval = indexSet.RefreshInterval
	}
	//创建索引表结构和设置类型
	mapping := mi{
		"settings": mi{
			"number_of_shards":   _shardsCount,
			"number_of_replicas": _replicasCount,
			"refresh_interval":   strconv.Itoa(_refreshInterval) + "s",
		},
		"mappings": mi{
			"properties": miTable,
		},
	}
	marshal, _ := json.Marshal(mapping)
	//flog.Println("json:", string(marshal))
	_, err := indexSet.data().CreateIndex(indexSet.indexName).BodyString(string(marshal)).Do(ctx)
	flog.Println("createindex:", err)
	//设置别名
	arrayAliName := strings.Split(indexSet.aliasesName, ",")
	for _, s := range arrayAliName {
		_, err2 := indexSet.data().Alias().Add(indexSet.indexName, s).Do(ctx)
		flog.Println("addalias:", err2)
	}
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
	if indexSet.es == nil {
		panic("elasticsearch初始化失败")
	}
	return indexSet.es
}

// Select 筛选字段
func (indexSet *IndexSet[Table]) Select(fields ...string) *IndexSet[Table] {
	indexSet.esService = indexSet.data().Search().Index(indexSet.indexName).FetchSourceContext(elastic.NewFetchSourceContext(true).Include(fields...))
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

// DelData 删除指定Id数据
func (indexSet *IndexSet[Table]) DelData(id string) error {
	_, err := indexSet.data().Delete().Index(indexSet.indexName).Id(id).Do(ctx)
	return err
}

// DelIndex 删除指定index索引数据
func (indexSet *IndexSet[Table]) DelIndex(indices ...string) error {
	_, err := indexSet.data().DeleteIndex(indices...).Do(ctx)
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
	indexSet.WhenNotExistsAddIndex(po)
	var err error
	poValueOf := reflect.ValueOf(po)
	Id := "0"
	for i := 0; i < poValueOf.NumField(); i++ {
		data := poValueOf.Type().Field(i).Tag.Get("es")
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
	if len(array) > 0 {
		indexSet.WhenNotExistsAddIndex(array[0])
	}
	//批量添加
	bulkRequest := indexSet.data().Bulk().Index(indexSet.indexName)
	for _, table := range array {
		poValueOf := reflect.ValueOf(table)
		Id := "0"
		for i := 0; i < poValueOf.NumField(); i++ {
			data := poValueOf.Type().Field(i).Tag.Get("es")
			if strings.HasPrefix(data, "primaryKey") {
				val := poValueOf.Field(i).Int()
				Id = strconv.FormatInt(val, 10)
				break
			}
		}
		req := elastic.NewBulkIndexRequest().Doc(table)
		req.Id(Id) //指定id
		bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	return err
}

// InsertList 插入列表形式
func (indexSet *IndexSet[Table]) InsertList(list collections.List[Table]) error {
	if list.Count() > 0 {
		indexSet.WhenNotExistsAddIndex(list.Index(0))
	}
	//批量添加
	bulkRequest := indexSet.data().Bulk().Index(indexSet.indexName)
	for i := 0; i < list.Count(); i++ {
		poValueOf := reflect.ValueOf(list.Index(i))
		Id := "0"
		for i := 0; i < poValueOf.NumField(); i++ {
			data := poValueOf.Type().Field(i).Tag.Get("es")
			if strings.HasPrefix(data, "primaryKey") {
				val := poValueOf.Field(i).Int()
				Id = strconv.FormatInt(val, 10)
				break
			}
		}
		req := elastic.NewBulkIndexRequest().Doc(list.Index(i))
		req.Id(Id) //指定id
		bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	return err
}

// ToList 转换List集合
func (indexSet *IndexSet[Table]) ToList() collections.List[Table] {
	if indexSet.esService == nil {
		indexSet.esService = indexSet.data().Search().Index(indexSet.indexName)
	}
	resp, _ := indexSet.esService.From(0).Size(10000).Do(ctx)
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
	if resp == nil {
		return collections.NewList[Table]()
	}
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
