package BeeDelayQueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"sync"
	"time"
)

//MassageListener 监听器
type MassageListener interface {
	//OnChange 增加变更监控
	OnMassage(msg *DelayMsg) bool
}

type DelayQueueController struct {
	retryTime  int64 `重试时间`
	retryCount int64 `重试次数`

	iNo       int32                `消息序号 1 - 9999`
	topicLock sync.RWMutex         `本地读写锁`
	topicList map[string]topicType `主题列表`
	ctrlRedis *redis.Client        `redis 控制服务`

	massageListenerKey  sync.RWMutex               `本地读写锁`
	massageListenerList map[string]MassageListener //每个主题的监听器只有一个
}

func NewDelayQueueController(redisConfig RedisConfig) (*DelayQueueController, error) {
	p := &DelayQueueController{iNo: 1}
	p.topicList = make(map[string]topicType)
	p.massageListenerList = make(map[string]MassageListener)
	p.ctrlRedis = redis.NewClient(&redis.Options{
		Addr:     redisConfig.Addr,
		Password: redisConfig.Password,
		DB:       redisConfig.Db,
	})
	go func() {
		for ; ; {
			p.work()
			time.Sleep(time.Second * 2)
		}
	}()
	rr := p.ctrlRedis.Set("sloth", "sloth", time.Duration(time.Millisecond))
	return p, rr.Err()
}

/*
* 创建一个延迟队列
*  topic  主题
*  RetryTime 重试时间
*  retryCount 重试次数
 */
func (p *DelayQueueController) CreateDelayQueue(topic string, retryTime int64, retryCount int64) {
	var tt topicType
	tt.TopicName = topic
	tt.TopicKey = GetTopicKey(topic)
	tt.TopicAckKey = GetTopicAckKey(topic)
	tt.IsRun = false
	p.retryTime = retryTime
	p.retryCount = retryCount
	p.topicList[topic] = tt
}

func (p *DelayQueueController) Push(topic string, body string, delaySecondTime int64) (string, error) {
	key := GetTopicKey(topic)
	var msg DelayMsg
	msg.Id = p.getTrackID()
	msg.Body = body
	msg.Topic = topic
	msg.RetryCount = 1
	msg.ExecTime = p.GetTimeUnix() + delaySecondTime
	msg.CreateTime = p.GetTimeUnix()
	r := p.ctrlRedis.ZAdd(key, redis.Z{float64(msg.ExecTime), p.toJson(msg)})
	return msg.Id, r.Err()
}
func (p *DelayQueueController) Ack(msg DelayMsg, tf bool) {
	if tf {
		fmt.Println("成功，删除Ack")
		p.deleteAckMsg(msg.Topic, p.toJson(msg)) //成功失败都删除
	} else {
		//大于重试次数丢弃
		if msg.RetryCount == p.retryCount {
			fmt.Println("丢弃:", msg)
			p.deleteAckMsg(msg.Topic, p.toJson(msg)) //成功失败都删除
			p.pushMsgDiscard(msg)
		}
	}
}

/*
*	添加到丢弃队列
 */
func (p *DelayQueueController) pushMsgDiscard(msg DelayMsg) {
	key := GetTopicDiscardKey(msg.Topic)
	p.ctrlRedis.ZAdd(key, redis.Z{float64(msg.CreateTime), p.toJson(msg)})
}

func (p *DelayQueueController) pushMsg(msg DelayMsg) (string, error) {
	key := GetTopicKey(msg.Topic)
	p.ctrlRedis.ZAdd(key, redis.Z{float64(msg.ExecTime), p.toJson(msg)})
	return msg.Id, nil
}

func (p *DelayQueueController) pushAckMsg(msgStr string) bool {
	var msg DelayMsg
	err := json.Unmarshal([]byte(msgStr), &msg)
	if err != nil {
		fmt.Println("json.Unmarshal() error:", err.Error())
		return false
	}
	newTime := time.Now().Unix() + p.retryTime
	key := GetTopicAckKey(msg.Topic)
	p.ctrlRedis.ZAdd(key, redis.Z{float64(newTime), p.toJson(msg)})
	return true
}

/*
* 消息监听器
 */
func (p *DelayQueueController) AddMassageListener(topic string, client MassageListener) error {
	p.massageListenerKey.Lock()
	p.massageListenerList[topic] = client
	p.massageListenerKey.Unlock()
	return errors.New("topic does not exist!")
}

func (p *DelayQueueController) DeleteDelayQueue(topic string) {
	/*
		if _, ok := p.topicList[topic]; ok {
		}
	*/

	p.topicLock.Lock()
	//删除主题队列
	delete(p.topicList, topic)
	//删除
	p.topicLock.Unlock()
}

/*
* 创建一个工作线程
 */
func (p *DelayQueueController) work() {
	p.topicLock.Lock()
	for k, topic := range p.topicList {
		if topic.IsRun == false { //只启动一次
			topic.IsRun = true
			p.topicList[k] = topic
			//给每个主题创建一个协成
			go p.toCallBack(topic) //回调推送消息
			go p.retryWork(topic)  //循环重试
			time.Sleep(time.Second * 1)
		}
	}
	p.topicLock.Unlock()
}

func (p *DelayQueueController) retryWork(topicInfo topicType) {
	for true {
		if p.isExit(topicInfo.TopicName) {
			return
		}
		p.ackLock(topicInfo.TopicName) //带锁,从队列中删除,防止别人获取到
		now := time.Now().Unix()
		list := p.ctrlRedis.ZRangeByScore(topicInfo.TopicAckKey, redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", now), Count: 1}).Val()
		for _, v := range list {
			var msg DelayMsg
			json.Unmarshal([]byte(v), &msg)
			if msg.RetryCount >= p.retryCount {
				//1.删除ACK
				p.ctrlRedis.ZRem(topicInfo.TopicAckKey, v).Val()
				//2.添加到丢弃队列
				p.pushMsgDiscard(msg)
			} else {
				//添加到正常列表,并且立即执行
				msg.RetryCount++
				msg.ExecTime = time.Now().Unix() //p.retryTime
				p.pushMsg(msg)
				//删除ack列表
				p.ctrlRedis.ZRem(topicInfo.TopicAckKey, v).Val()
			}
		}
		p.ackUnLock(topicInfo.TopicName)
		if len(list) > 0 {
			continue
		}
		time.Sleep(time.Millisecond * 500)
	}
}

/*
* 回调推送消息
 */
func (p *DelayQueueController) toCallBack(topicInfo topicType) {
	//1.等待监听者
	var client MassageListener
	for true {
		if p.isExit(topicInfo.TopicName) {
			return
		}
		client = p.getListener(topicInfo.TopicName)
		if client == nil {
			time.Sleep(1 * time.Second)
			//在没有监听者的时候就等待
			continue
		}
		break
	}
	for true {
		if p.isExit(topicInfo.TopicName) {
			return
		}
		list := p.getQueueMsg(topicInfo.TopicName, topicInfo.TopicKey)
		for _, v := range list {
			//1.存在消息 => 回调客户端,
			var msg DelayMsg
			err := json.Unmarshal([]byte(v), &msg)
			if err != nil {
				//fmt.Println("json.Unmarshal() error:", err.Error())
				continue
			}
			go client.OnMassage(&msg)
		}
		if len(list) > 0 {
			//这里就是为了,让出竞争的锁
			time.Sleep(time.Millisecond * 20)
			continue
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (p *DelayQueueController) isExit(topicName string) bool {
	//主题队列退出
	p.topicLock.RLock()
	if _, ok := p.topicList[topicName]; !ok {
		//停止队列
		fmt.Println(topicName, "队列退出")
		p.topicLock.RUnlock()
		return true
	}
	p.topicLock.RUnlock()
	return false
}

/*
* 获取主题队列的消息
 */
func (p *DelayQueueController) getQueueMsg(topicName string, topicKey string) []string {
	now := time.Now().Unix()
	p.lock(topicName)
	//1.每次获取一个
	list := p.ctrlRedis.ZRangeByScore(topicKey, redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", now), Count: 1}).Val()
	//带锁,从队列中删除,防止别人获取到
	var newList []string
	for _, v := range list {
		if p.pushAckMsg(v) { //一般不会失败
			p.ctrlRedis.ZRem(topicKey, v).Val()
			newList = append(newList, v)
		}
	}
	p.unLock(topicName)
	return newList
}

/*
* 获取主题的监听者
 */
func (p *DelayQueueController) getListener(topicName string) MassageListener {
	p.massageListenerKey.RLock()
	if _, ok := p.massageListenerList[topicName]; !ok {
		p.massageListenerKey.RUnlock()
		return nil
	}
	client := p.massageListenerList[topicName]
	p.massageListenerKey.RUnlock()
	return client
}

//删除消息
func (p *DelayQueueController) deleteAckMsg(topicName, msg string) bool {
	p.ackLock(topicName)
	key := GetTopicAckKey(topicName)
	p.ctrlRedis.ZRem(key, msg).Val()
	p.ackUnLock(topicName)
	return true
}

/*
* redis 锁
 */
func (p *DelayQueueController) lock(topicName string) {
	key := GetTopicLockKey(topicName)
	for !p.ctrlRedis.SetNX(key, topicName, 3*time.Second).Val() {
		time.Sleep(100 * time.Millisecond)
	}
}

/*
* redis 解锁
 */
func (p *DelayQueueController) unLock(topicName string) {
	key := GetTopicLockKey(topicName)
	p.ctrlRedis.Del(key)
}

/*
* redis 锁
 */
func (p *DelayQueueController) ackLock(topicName string) bool {
	key := GetTopicAckLockKey(topicName)
	for !p.ctrlRedis.SetNX(key, topicName, 3*time.Second).Val() {
		time.Sleep(1 * time.Second)
	}
	return true
}

/*
* redis 解锁
 */
func (p *DelayQueueController) ackUnLock(topicName string) {
	key := GetTopicAckLockKey(topicName)
	p.ctrlRedis.Del(key)
}

func (p *DelayQueueController) getTrackID() string {
	if p.iNo >= 9999 {
		p.iNo = 1
	}
	id := GetTrackID(p.iNo)
	p.iNo++
	return id
}

func (p *DelayQueueController) toJson(msg interface{}) string {
	jsonStr, _ := json.Marshal(msg)

	return string(jsonStr)
}
func (p *DelayQueueController) GetTime() (*time.Time, error) {
	rTime := p.ctrlRedis.Time()
	if rTime.Err() != nil {
		return nil, rTime.Err()
	}
	rrTime := rTime.Val()
	return &rrTime, nil
}
func (p *DelayQueueController) GetTimeUnix() int64 {
	rTime := p.ctrlRedis.Time()
	if rTime.Err() != nil {
		//return 0, rTime.Err()
		return time.Now().Unix()
	}
	timeStamp := rTime.Val().Unix()
	return timeStamp
}
