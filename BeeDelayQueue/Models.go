package BeeDelayQueue

type topicType struct {
	TopicName   string //主题名称
	TopicKey    string //主题的key
	TopicAckKey string //主题的key
	IsRun       bool
}

//消息体
type DelayMsg struct {
	Id         string `json:"id"`         //消息唯一标识
	Topic      string `json:"topic"`      //消息主题
	Body       string `json:"body"`       //消息内容
	ExecTime   int64  `json:"execTime"`   //需要执行的时间戳 秒级别
	RetryCount int64  `json:"retryCount"` //重试次数
	CreateTime int64  `json:"createTime"` //消息的创建时间
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}
