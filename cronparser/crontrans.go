package cronparser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

//CronTranslate 参数：时间策略的类型,细节,返回crontab表达式(cronstring)和时间策略详情描述(crondetail)
func CronTranslate(scheduleType E_SCHEDULE_TYPE, scheduleDetail string) (crontab string, description string, err error) {

	switch scheduleType {
	//每隔一定时间
	case EVERY_FEW:

		//example: 1min 2hour 3day 4week 5month
		if strings.Contains(scheduleDetail, "min") {
			times := strings.Split(scheduleDetail, "min")
			if times[0] == "" {
				return "", "", errors.New("valid schedule: " + scheduleDetail)
			}

			description = fmt.Sprintf("每隔%s分钟", times[0])

			crontab = fmt.Sprintf("*/%s * * * *", times[0])
		}

		if strings.Contains(scheduleDetail, "hour") {
			times := strings.Split(scheduleDetail, "hour")
			if times[0] == "" {
				return "", "", errors.New("valid schedule: " + scheduleDetail)
			}
			description = fmt.Sprintf("每隔%s小时", times[0])

			crontab = fmt.Sprintf("0 */%s * * *", times[0])
		}

		if strings.Contains(scheduleDetail, "day") {
			times := strings.Split(scheduleDetail, "day")
			if times[0] == "" {
				return "", "", errors.New("valid schedule: " + scheduleDetail)
			}
			description = fmt.Sprintf("每隔%s天", times[0])

			crontab = fmt.Sprintf("0 0 */%s * *", times[0])
		}

		if strings.Contains(scheduleDetail, "week") {
			times := strings.Split(scheduleDetail, "week")
			if times[0] == "" {
				return "", "", errors.New("valid schedule: " + scheduleDetail)
			}
			weeks, _ := strconv.Atoi(times[0])
			description = fmt.Sprintf("每隔%s周", times[0])

			crontab = fmt.Sprintf("0 0 */%s * 0", strconv.Itoa(weeks*7))
		}
		if strings.Contains(scheduleDetail, "month") {
			times := strings.Split(scheduleDetail, "month")
			if times[0] == "" {
				return "", "", errors.New("valid schedule: " + scheduleDetail)
			}
			description = fmt.Sprintf("每隔%s个月", times[0])

			crontab = fmt.Sprintf("0 0 1 */%s *", times[0])
		}

	//每天
	case EVERY_DAY:
		//example: 09:23:00 每天9点23分
		timeParse, err := time.Parse("15:04", scheduleDetail)
		if err != nil {
			return "", "", err
		}
		description = fmt.Sprintf("每天 %s", scheduleDetail)
		crontab = fmt.Sprintf("%s %s * * *", strconv.Itoa(timeParse.Minute()), strconv.Itoa(timeParse.Hour()))

	//每周
	case EVERY_WEEK:
		//example: 3,09:23:00 每周三9点23分
		dates := strings.Split(scheduleDetail, ",")
		description = fmt.Sprintf("每周%s %s", dates[0], dates[1])

		week := dates[0]
		timeParse, err := time.Parse("15:04", dates[1])
		if err != nil {
			return "", "", err
		}
		if week == "7" {
			week = 0
		}
		crontab = fmt.Sprintf("%s %s * * %s", strconv.Itoa(timeParse.Minute()), strconv.Itoa(timeParse.Hour()), week)

	//每月
	case EVERY_MONTH:
		//scheduleDetail: 23,09:23:00 每月23号9点23分
		dates := strings.Split(scheduleDetail, ",")
		description = fmt.Sprintf("每月%s号 %s", dates[0], dates[1])

		date := dates[0]
		timeParse, err := time.Parse("15:04", dates[1])
		if err != nil {
			return "", "", err
		}
		crontab = fmt.Sprintf("%s %s %s * *", strconv.Itoa(timeParse.Minute()), strconv.Itoa(timeParse.Hour()), date)

	//自定义: * * * * *
	case CUSTOM_SCHEDULE:
		crontab = scheduleDetail
		description = fmt.Sprintf("自定义: %s", scheduleDetail)

	case NO_SCHEDULE:
		crontab = ""
		description = "无"
	default:
		crontab = ""
		description = "无"
	}

	return crontab, description, nil
}
