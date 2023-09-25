package elasticSearch

// elasticSearch配置结构
type elasticConfig struct {
	Server          string // 服务器地址
	Username        string // 用户名称
	Password        string // 用户密码
	ReplicasCount   int    // 副本数量
	ShardsCount     int    // 碎片数量
	RefreshInterval int    // 刷新间隔
	IndexFormat     string // 索引格式
}
