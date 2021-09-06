package TestFail

import (
	"BeeDelayQueue/BeeDelayQueue"
	"fmt"
	"testing"
	"time"
)

type MassageListener struct {
	delayClient *BeeDelayQueue.DelayQueueController
}

func (p *MassageListener) Set(delayClient *BeeDelayQueue.DelayQueueController)  {
	p.delayClient = delayClient
}

func (p *MassageListener) OnMassage(msg *BeeDelayQueue.DelayMsg) bool {
	fmt.Println("收到消息 : ", msg)
	time.Sleep(time.Second * 5)
	p.delayClient.Ack(*msg,false)
	return false
}

func TestAddMsg(t *testing.T) {
	topic := "orderCancel"
	var  redisConfig BeeDelayQueue.RedisConfig
	redisConfig.Addr = "127.0.0.1:6379"
	redisConfig.Db = 1
	delayClient,_ := BeeDelayQueue.NewDelayQueueController(redisConfig)
	//1.创建主题队列
	delayClient.CreateDelayQueue(topic, 5,3)
	//2.添加监听者
	var massageListener MassageListener
	massageListener.Set(delayClient)
	delayClient.AddMassageListener(topic, &massageListener)
	//3.添加消息
	body := "{\"partnerId\":\"2399\",\"shopId\":\"hk001\",\"data\":{\"tp_order_id\":\"21750055968970500800003\"}}"
	id, err := delayClient.Push(topic, body, 5)
	if err != nil {
		fmt.Println("error:", err.Error())
		return
	}
	fmt.Println("创建消息 : ", id)

	/*
	chan1 := make(chan bool)
	<-chan1
	 */
	time.Sleep(30 * time.Second)
}
