package cronparser

import (
	"fmt"
	"strings"
)

//CronParse cron解析
func CronParse(description string) (scheduleType int, scheduleDetail string, err error) {

	if description == "无" {
		scheduleType = int(NO_SCHEDULE)
		scheduleDetail = ""
	}
	//每隔1分
	if strings.HasPrefix(description, "每隔") {
		scheduleType = int(EVERY_FEW)
		if strings.Contains(description, "分钟") {
			str1 := strings.Split(description, "每隔")
			str2 := strings.Split(str1[1], "分钟")
			scheduleDetail = fmt.Sprintf("%smin", str2[0])
		}
		if strings.Contains(description, "小时") {
			str1 := strings.Split(description, "每隔")
			str2 := strings.Split(str1[1], "小时")
			scheduleDetail = fmt.Sprintf("%shour", str2[0])
		}
		if strings.Contains(description, "天") {
			str1 := strings.Split(description, "每隔")
			str2 := strings.Split(str1[1], "天")
			scheduleDetail = fmt.Sprintf("%sday", str2[0])

		}
		if strings.Contains(description, "周") {
			str1 := strings.Split(description, "每隔")
			str2 := strings.Split(str1[1], "周")
			scheduleDetail = fmt.Sprintf("%sweek", str2[0])
		}
		if strings.Contains(description, "月") {
			str1 := strings.Split(description, "每隔")
			str2 := strings.Split(str1[1], "月")
			scheduleDetail = fmt.Sprintf("%smonth", str2[0])
		}
	}

	//每天 09:23
	if strings.HasPrefix(description, "每天 ") {
		scheduleType = int(EVERY_DAY)
		str := strings.Split(description, "每天 ")
		scheduleDetail = str[1]
	}

	//每周3 09:23
	if strings.HasPrefix(description, "每周") {
		scheduleType = int(EVERY_WEEK)
		str := strings.Split(description, "每周")
		scheduleDetail = strings.Replace(str[1], " ", ",", 1)
	}

	//每月23号 09:23
	if strings.HasPrefix(description, "每月") {
		scheduleType = int(EVERY_MONTH)
		str := strings.Split(description, "每月")
		scheduleDetail = strings.Replace(str[1], "号 ", ",", 1)
	}

	//自定义
	if strings.HasPrefix(description, "自定义") {
		scheduleType = int(CUSTOM_SCHEDULE)
		str := strings.Split(description, "自定义: ")
		scheduleDetail = str[1]
	}

	return scheduleType, scheduleDetail, nil
}
