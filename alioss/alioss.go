package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-ini/ini"
)

var endPoint = "Endpoint"
var akId = "AccessKeyId"
var akS = "AccessKeySecret"
var pngDir []string
var bucket string

func main() {
	confPath := os.Args[1]

	LoadConf(confPath)

	fmt.Println("Endpoint:", endPoint)
	fmt.Println("AccessKeyId:", akId)
	fmt.Println("AccessKeySecret:", akS)

	fmt.Println("开始认证")
	client, err := oss.New(endPoint, akId, akS)
	if err != nil {
		fmt.Println("认证失败")
		return
	}
	fmt.Println("认证成功")

	fmt.Println("尝试获取bucket:", bucket)
	bucket, err := client.Bucket(bucket)
	if err != nil {
		fmt.Println("bucker获取失败")
		return
	}
	fmt.Println("连接bucket成功")

	for _, v := range pngDir {
		fmt.Println("开始上传", v, "下的文件")
		go uploadOss(bucket, v)
	}
	fmt.Println("全部成功")
	return
}

//uploadOss 上传文件
func uploadOss(bucket *oss.Bucket, pngDir string) error {
	var pathName map[string]string
	pathName = make(map[string]string)

	err := filepath.Walk(pngDir, func(filePath string, fi os.FileInfo, err error) error {
		if err != nil { //忽略错误
			return err
		}
		if fi.IsDir() { // 忽略目录
			return nil
		}
		fileName := fi.Name()
		pathName[filePath] = fileName
		return nil
	})

	if err != nil {
		fmt.Println(err)
		return err
	}
	for k, v := range pathName {
		err := bucket.PutObjectFromFile(v, k)
		if err != nil {
			fmt.Println(k, "上传失败")
		}
	}
	return nil
}

//加载ali oss配置文件
func LoadConf(conf string) {

	cfg, err := ini.Load(conf)
	if err != nil {
		fmt.Println("配置文件读取失败")
	}

	ssss, _ := cfg.GetSection("")

	enddd, _ := ssss.GetKey("Endpoint")
	endPoint = enddd.Value()

	akkk, _ := ssss.GetKey("AccessKeyId")
	akId = akkk.Value()

	aksss, _ := ssss.GetKey("AccessKeySecret")
	akS = aksss.Value()

	buckk, _ := ssss.GetKey("Bucket")
	bucket = buckk.Value()

	pnggg, _ := ssss.GetKey("PngDir")
	png := pnggg.Value()
	pngDir = strings.Split(png, ",")

	return
}
