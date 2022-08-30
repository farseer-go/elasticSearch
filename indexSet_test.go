package elasticSearch

import (
	"reflect"
	"testing"
)

type UserPO struct {
	Id int `gorm:"primaryKey"`
	// 用户名称
	Name string
	// 用户年龄
	Age int
}

func TestInit(t *testing.T) {
	po := UserPO{Age: 20, Name: "小小", Id: 100}

	typeOfPo := reflect.TypeOf(po)
	valueOfPo := reflect.ValueOf(po)
	// 通过 #NumField 获取结构体字段的数量
	for i := 0; i < typeOfPo.NumField(); i++ {
		key := typeOfPo.Field(i).Name
		value := valueOfPo.Field(i)
		flog.Info("key :", key, " value: ", value)
	}

}
