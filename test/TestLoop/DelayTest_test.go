package TestLoop

import (
	"BeeDelayQueue/BeeDelayQueue"
	"fmt"
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
	fmt.Println("1***** 收到消息 : ", msg.Id)
	time.Sleep(time.Millisecond * 50)
	p.delayClient.Ack(*msg, true)
	return true
}

func TestClinet1(t *testing.T) {
	topic := "orderCancel"

	var redisConfig BeeDelayQueue.RedisConfig
	redisConfig.Addr = "127.0.0.1:6379"
	redisConfig.Db = 1


	//2.添加监听者
	/*
		{
			var massageListener MassageListener
			massageListener.Set(delayClient)
			delayClient.AddMassageListener(topic, &massageListener)
		}
	*/
	//3.添加消息
	for i := 0; i < 10; i++ {
		go func() {
			var iCount int32
			delayClient, _ := BeeDelayQueue.NewDelayQueueController(redisConfig)
			//1.创建
			delayClient.CreateDelayQueue(topic, 10, 3)
			for true {
				if iCount > 600000 {
					break
				}
				body := "{\"partnerId\":\"2399\",\"shopId\":\"hk001\",\"data\":{\"tp_order_id\":\"21750055968970500800003\"}}"
				id, err := delayClient.Push(topic, body, 10)
				if err != nil {
					fmt.Println("error:", err.Error())
					return
				}
				fmt.Println("创建消息 : ", id)
				time.Sleep(time.Millisecond * 5)
				iCount++
			}
		}()

	}

	//time.Sleep(30 * time.Second)

	chan1 := make(chan bool)
	<-chan1

}

func TestClinet2(t *testing.T) {

	for i := 0; i < 10; i++ {
		clinetName := fmt.Sprintf("Clinet_%v", i)
		topic := "orderCancel"

		var redisConfig BeeDelayQueue.RedisConfig
		redisConfig.Addr = "127.0.0.1:6379"
		redisConfig.Db = 1
		delayClient, _ := BeeDelayQueue.NewDelayQueueController(redisConfig)

		//1.创建
		delayClient.CreateDelayQueue(topic, 10, 3)
		//2.添加监听者
		{
			var massageListener MassageListener2
			massageListener.Set(delayClient, clinetName)
			delayClient.AddMassageListener(topic, &massageListener)
		}
	}
	chan1 := make(chan bool)
	<-chan1
}

type MassageListener2 struct {
	id          string
	delayClient *BeeDelayQueue.DelayQueueController
}

func (p *MassageListener2) Set(delayClient *BeeDelayQueue.DelayQueueController, id string) {
	p.delayClient = delayClient
	p.id = id
}
func (p *MassageListener2) OnMassage(msg *BeeDelayQueue.DelayMsg) bool {
	fmt.Println(p.id, "***** 收到消息 : ", msg.Id)
	p.delayClient.Ack(*msg, true)
	return true
}
