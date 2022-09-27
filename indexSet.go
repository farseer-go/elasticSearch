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
	client          *elastic.Client
	searchService   *elastic.SearchService
	queryArray      []elastic.Query
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

// 初始化es
func (indexSet *IndexSet[Table]) data() *elastic.SearchService {
	indexSet.getClient()
	return indexSet.searchService
}

// 初始化client
func (indexSet *IndexSet[Table]) getClient() *elastic.Client {
	if indexSet.client == nil {
		es, _ := elastic.NewClient(
			elastic.SetURL(indexSet.esContext.esConfig.Server),
			elastic.SetBasicAuth(indexSet.esContext.esConfig.Username, indexSet.esContext.esConfig.Password),
			elastic.SetSniff(false), //非集群下，关闭嗅探机制
		)

		if es == nil {
			panic("elasticsearch初始化失败")
		}

		indexSet.client = es
		indexSet.searchService = es.Search().Index(indexSet.aliasesName)
	}
	return indexSet.client
}

// SetAliasesName 设置别名
func (indexSet *IndexSet[Table]) SetAliasesName(aliasesName string) error {
	_, err := indexSet.getClient().Alias().Add(indexSet.indexName, aliasesName).Do(ctx)
	return err
}

// WhenNotExistsAddIndex 当不存在的时候创建索引
func (indexSet *IndexSet[Table]) WhenNotExistsAddIndex(po Table) {
	indexSet.data()
	do, _ := indexSet.client.IndexExists(indexSet.indexName).Do(ctx)
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
	_, err := indexSet.getClient().CreateIndex(indexSet.indexName).BodyString(string(marshal)).Do(ctx)
	flog.Println("createindex:", err)

	//设置别名
	arrayAliName := strings.Split(indexSet.aliasesName, ",")
	for _, s := range arrayAliName {
		err2 := indexSet.SetAliasesName(s)
		flog.Println("addalias:", err2)
	}
}

// Select 筛选字段
func (indexSet *IndexSet[Table]) Select(fields ...string) *IndexSet[Table] {
	indexSet.searchService = indexSet.data().FetchSourceContext(elastic.NewFetchSourceContext(true).Include(fields...))
	return indexSet
}

// Asc 正序排序
func (indexSet *IndexSet[Table]) Asc(field string) *IndexSet[Table] {
	indexSet.searchService = indexSet.data().Sort(field, true)
	return indexSet
}

// Desc 倒序排序
func (indexSet *IndexSet[Table]) Desc(field string) *IndexSet[Table] {
	indexSet.searchService = indexSet.data().Sort(field, false)
	return indexSet
}

// DelData 删除指定Id数据
func (indexSet *IndexSet[Table]) DelData(id string) error {
	_, err := indexSet.getClient().Delete().Index(indexSet.indexName).Id(id).Do(ctx)
	return err
}

// DelIndex 删除指定index索引数据
func (indexSet *IndexSet[Table]) DelIndex(indices ...string) error {
	_, err := indexSet.getClient().DeleteIndex(indices...).Do(ctx)
	return err
}

// Where 倒序排序
func (indexSet *IndexSet[Table]) Where(field string, fieldValue any) *IndexSet[Table] {
	switch fieldValue.(type) {
	case nil:
		return indexSet
	case string:
		if fieldValue == "" {
			return indexSet
		}
	}
	termQuery := elastic.NewTermQuery(field, fieldValue)
	indexSet.queryArray = append(indexSet.queryArray, termQuery)
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
	_, err = indexSet.getClient().Index().Index(indexSet.indexName).Id(Id).BodyJson(po).Do(ctx)
	return err
}

// InsertArray 数组的形式插入
func (indexSet *IndexSet[Table]) InsertArray(array []Table) error {
	if len(array) > 0 {
		indexSet.WhenNotExistsAddIndex(array[0])
	}
	//批量添加
	bulkRequest := indexSet.getClient().Bulk().Index(indexSet.indexName)
	for i := 0; i < len(array); i++ {
		poValueOf := reflect.ValueOf(array[i])
		var id int64
		for i := 0; i < poValueOf.NumField(); i++ {
			data := poValueOf.Type().Field(i).Tag.Get("es")
			if strings.HasPrefix(data, "primaryKey") {
				id = poValueOf.Field(i).Int()
				break
			}
		}
		req := elastic.NewBulkIndexRequest().Doc(array[i])
		if id > 0 {
			req.Id(strconv.FormatInt(id, 10)) //指定id
		}
		bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	return err
}

// InsertList 插入列表形式
func (indexSet *IndexSet[Table]) InsertList(list collections.List[Table]) error {
	return indexSet.InsertArray(list.ToArray())
}

// ToList 转换List集合
func (indexSet *IndexSet[Table]) ToList() collections.List[Table] {
	boolQuery := elastic.NewBoolQuery().Must()
	if len(indexSet.queryArray) > 0 {
		boolQuery.Must(indexSet.queryArray...)
	}
	resp, _ := indexSet.data().Query(boolQuery).From(0).Size(10000).Do(ctx)
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
func (indexSet *IndexSet[Table]) ToPageList(pageSize int, pageIndex int) collections.PageList[Table] {
	boolQuery := elastic.NewBoolQuery().Must()
	if len(indexSet.queryArray) > 0 {
		boolQuery.Must(indexSet.queryArray...)
	}
	resp, _ := indexSet.data().Query(boolQuery).From((pageIndex - 1) * pageSize).Size(pageSize).Pretty(true).Do(ctx)
	lst := collections.NewList[Table]()

	if resp == nil {
		return collections.NewPageList[Table](lst, 0)
	}

	for _, hit := range resp.Hits.Hits {
		var entity Table
		_ = json.Unmarshal(hit.Source, &entity)
		lst.Add(entity)
	}
	return collections.NewPageList[Table](lst, resp.Hits.TotalHits.Value)
}
