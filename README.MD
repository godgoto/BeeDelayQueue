# BeeDelayQueue 消息延迟队列

### 一、应用场景：

    1.订单超过 30 分钟未支付，则自动取消。
    2.外卖商家超时未接单，则自动取消。
    3.延迟查询自动售货机的出货状态。
    等等场景都可以用定时任务去轮询实现，但是当数据量过大的时候，高频轮询数据库会消耗大量的资源，此时用延迟队列来应对这类场景比较好。

### 二、需求

    消息存储
    过期延时消息实时获取
    高可用性

#### 2.3、Redis 延迟队列

    消息持久化，消息至少被消费一次
    实时性：存在一定的时间误差（定时任务间隔）
    支持指定消息 remove
    高可用性
    Redis 的特殊数据结构 ZSet 满足延迟的特性

### 3.Redis 延迟队里的实现

    使用sortedset，用时间戳作为score，使用zadd key score1 value1命令生产消息，使用zrangebysocre key min max withscores limit 0 1消费消息最早的一条消息。
    redis  相关扩展
    3.1、使用 sortedset 操作元素
        赋值：zadd key score1 value1 score2 value2... (把全部的元素添加到sorted set中，并且每个元素有其对应的分数，返回值是新增的元素个数。)
    3.2获取元素：
        zscore key value：返回指定成员的分数
        zcard key : 获取集合中的成员数量
    3.3 删除元素：zrem key value1 value2 … 删除指定元素
        zremrangebyrank key start stop：按照排名范围删除元素。
        zremrangebyscore key min max：按照分数范围删除元素。
    3.4 查询元素：
        zrange key start end withscores：查询start到end之间的成员。
        zrevrange key start end withscores：查询成员分数从大到小顺序的索引 start 到 end 的所有成员。
        zrangebyscore key min max withscores limit offset count：返回分数 min 到 max 的成员并按照分数从小到大排序, limit 是从 offset 开始展示几个元素。
        Acknowledge characte

### 4.Redis 实现 ACK 功能

       4.1 需要在业务代码中处理消息失败的情况，回滚消息到原始等待队列。
            Consumer 挂掉，仍然需要回滚消息到等待队列中。
            前者只需要在业务中处理消费异常的情况，后者则需要维护两个队列。
       4.2 Redis ACK 实现方案
            1.维护一个消息记录表，存贮消息的消费记录，用于失败时回滚消息。表中记录消息ID、消息内容、消息时间、消息状态。
            定时任务轮询该消息表，处理消费记录表中消费状态未成功的记录，重新放入等待队列。
            2.维护一个ACK表,投递消息后 加入 ack队列,收到ACK请求 失败： 删除,ACK列表,重新回到队列中,成功则删除ACK,定时维护ACK列表,过期自动加入
        4.3. 使用zrangebysocre key min max  limit 0 1消费消息最早的一条消息,如果超时了,修改,ExecTime,返回正常的消息队列!

### 5.多实例
    reids 使用 : github.com/go-redis/redis
    利用 Redis 的 SETNX 设置过期时间,做分布式锁,防止死锁!

### 6.消息ID生成

    基础做法 日期20210826  + 3位的进程号 + 时间戳1571987125435 + 4位序号

### 7. Discard 丢弃队列

    超过失败重试次数的时候,写入丢弃队列 :Delay:Queue:Order:Discard:orderCancel_20210827
    我们为每一个主题队列每天创建一个队列,后续可以把失败队列写入数据库的功能,失败队列的分数会用的创建时间

### 8. 使用

#### 8.1 引入
    import "github.com/godgoto/BeeDelayQueue"
#### 8.2 监听者

```cgo
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

```

#### 8.3 创建主题队列

```cgo
    //1.new
    var redisConfig BeeDelayQueue.RedisConfig
    redisConfig.Addr = "127.0.0.1:6379"
    redisConfig.Db = 1
    delayClient, _ := BeeDelayQueue.NewDelayQueueController(redisConfig)
    //2.创建一个主题队列
    topic := "orderCancel"
    delayClient.CreateDelayQueue(topic, 10, 3)
    //3.添加监听者
    var massageListener MassageListener
    massageListener.Set(delayClient)
    delayClient.AddMassageListener(topic, &massageListener)
    //4.添加消息
    body := "{\"partnerId\":\"2399\",\"shopId\":\"s001\",\"data\":{\"tp_order_id\":\"21750055968970500800003\"}}"
    timeStamp,_ := delayClient.GetTimeUnix()
    execTime := timeStamp + 5
    fmt.Println("timeStamp:",timeStamp, " ", execTime)
    id, err := delayClient.Push(topic, body, execTime)
    if err != nil {
        fmt.Println("创建消息失败 : error:", err.Error())
        return
    } else {
        fmt.Println("创建消息成功 id : ", id)
    }
    chan1 := make(chan bool)
    <-chan1
```

#### 9.1 感谢！
    redis : github.com/go-redis/redis   