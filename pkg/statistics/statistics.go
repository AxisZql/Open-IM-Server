package statistics

import (
	"Open_IM/pkg/common/log"
	"time"
)

type Statistics struct {
	AllCount   *uint64
	ModuleName string
	PrintArgs  string
	SleepTime  uint64 // output休眠时间间隔 axis
}

func (s *Statistics) output() {
	var intervalCount uint64
	t := time.NewTicker(time.Duration(s.SleepTime) * time.Second)
	defer t.Stop()
	var sum uint64
	// 记录一共经历的时间间隔数
	var timeIntervalNum uint64
	for {
		sum = *s.AllCount
		select {
		case <-t.C:
		}
		if *s.AllCount-sum <= 0 {
			intervalCount = 0
		} else {
			intervalCount = *s.AllCount - sum
		}
		timeIntervalNum++
		// 输出每个间隔内的消息推送速率 axis
		log.NewWarn("", " system stat ", s.ModuleName, s.PrintArgs, intervalCount, "total:", *s.AllCount, "intervalNum", timeIntervalNum, "avg", (*s.AllCount)/(timeIntervalNum)/s.SleepTime)
	}
}

func NewStatistics(allCount *uint64, moduleName, printArgs string, sleepTime int) *Statistics {
	p := &Statistics{AllCount: allCount, ModuleName: moduleName, SleepTime: uint64(sleepTime), PrintArgs: printArgs}
	go p.output()
	return p
}
