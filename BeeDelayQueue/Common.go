package BeeDelayQueue

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

/*
* 获取主题队列的key
 */
func GetTopicKey(topic string) string {
	return fmt.Sprintf("BeeDelay:Queue:Order:%v", topic)
}

/*
* 主题队里的锁
 */
func GetTopicLockKey(topic string) string {
	return fmt.Sprintf("BeeDelay:Queue:Lock:%v", topic)
}

/*
* 获取主题ACK队列的key
 */
func GetTopicAckKey(topic string) string {
	return fmt.Sprintf("BeeDelay:Queue:Order:%v_Ack", topic)
}

/*
*  获取主题ACK队列的锁
 */
func GetTopicAckLockKey(topic string) string {
	return fmt.Sprintf("BeeDelay:Queue:Kock:%v_Aak", topic)
}

/*
* 丢弃的主题key
 */
func GetTopicDiscardKey(topic string) string {
	return fmt.Sprintf("BeeDelay:Queue:Order:Discard:%v_%v", topic, GetFormatTime(time.Now()))
}

func GetFormatTime(time time.Time) string {
	return time.Format("20060102")
}

// 基础做法 日期20210826  + 3位的进程号 + 时间戳1571987125435 + goroutineID + 4位序号
func GetTrackID(iNo int32) string {
	date := GetFormatTime(time.Now())
	//randNum, _ := rand.Int(rand.Reader, big.NewInt(1000))
	// sup(int32(randNum.Int64())
	code := fmt.Sprintf("%s-%v-%d-%v-%v", date, sup(int32(os.Getpid()%1000), 3), time.Now().UnixNano(),sup(int32(GetGoroutineID()),3), sup(iNo, 4))
	return code
}
func sup(i int32, n int) string {
	m := fmt.Sprintf("%d", i)
	for len(m) < n {
		m = fmt.Sprintf("0%s", m)
	}
	return m
}

func GetGoroutineID() int64 {
	b := make([]byte, 64)
	runtime.Stack(b, false)
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseInt(string(b), 10, 64)
	return n
}
