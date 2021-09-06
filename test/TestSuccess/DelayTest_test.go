package TestSuccess

import (
	"BeeDelayQueue/BeeDelayQueue"
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"testing"
	"time"
)

type MassageListener struct {
	delayClient *BeeDelayQueue.DelayQueueController
}

func (p *MassageListener) Set(delayClient *BeeDelayQueue.DelayQueueController) {
	p.delayClient = delayClient
}
func (p *MassageListener) OnMassage(msg *BeeDelayQueue.DelayMsg) bool {
	fmt.Println("收到消息 : ", msg)
	p.delayClient.Ack(*msg, true)
	return true
}

func TestAddMsg(t *testing.T) {
	topic := "orderCancel"
	var redisConfig BeeDelayQueue.RedisConfig
	redisConfig.Addr = "127.0.0.1:6379"
	redisConfig.Db = 1
	delayClient, _ := BeeDelayQueue.NewDelayQueueController(redisConfig)
	//1.创建
	delayClient.CreateDelayQueue(topic, 10, 3)
	//2.添加监听者
	var massageListener MassageListener
	massageListener.Set(delayClient)
	delayClient.AddMassageListener(topic, &massageListener)
	//3.添加消息
	body := "{\"partnerId\":\"2399\",\"shopId\":\"s001\",\"data\":{\"tp_order_id\":\"21750055968970500800003\"}}"
	id, err := delayClient.Push(topic, body, 5)
	if err != nil {
		fmt.Println("创建消息失败 : error:", err.Error())
		return
	} else {
		fmt.Println("创建消息成功 id : ", id)
	}
	time.Sleep(6 * time.Second)
}


func Test2(t *testing.T) {
	ctrlRedis := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       1,
	})
	str  := ctrlRedis.Get("XXXX").Val()
	fmt.Println("str:",str)

	fmt.Println(GetTodayTime())
}

func  GetTodayTime()  int{
	t := time.Now()
	zero_tm := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
	strInt64 := strconv.FormatInt(zero_tm, 10)
	//fmt.Println(reflect.TypeOf(strInt64))
	id16 ,_ := strconv.Atoi(strInt64)

	return id16
}




func Test3(t *testing.T) {
	ctrlRedis := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       1,
	})

	for i:=0;i<2;i++ {
		key := "1111"
		sTime := time.Now().Unix()
		for !ctrlRedis.SetNX(key, key, 30*time.Second).Val() {
			time.Sleep(100 * time.Millisecond)
		}
		eTime := time.Now().Unix()
		ctrlRedis.Del(key)

		fmt.Println("s:",sTime,"e:",eTime ,"  ic:",eTime - sTime)

	}


}
func lock(key string) {

}

/*
* redis 解锁
 */
func unLock(topicName string) {

}
