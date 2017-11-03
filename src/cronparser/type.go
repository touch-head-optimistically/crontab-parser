package cronparser

//E_SCHEDULE_TYPE  定时策略类型
type E_SCHEDULE_TYPE int

const (
	EVERY_FEW       E_SCHEDULE_TYPE = iota //value 0 每隔某些时间
	EVERY_DAY                              //value 1 每天某个时间
	EVERY_WEEK                             //value 2 每周某个时间
	EVERY_MONTH                            //value 3 每月某个时间
	CUSTOM_SCHEDULE                        //value 4 自定义高级 **/1***
	NO_SCHEDULE                            //value 5 无
)
