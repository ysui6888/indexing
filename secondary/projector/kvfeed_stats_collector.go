// Connector connecting KVFeed to downstream nodes, i.e., KVFeed

package projector

import (
    "sync"
	pc "github.com/Xiaomei-Zhang/goxdcr/common"
	c "github.com/couchbase/indexing/secondary/common"
)

// Collector of KVFeed statistics
type KVFeedStatisticsCollector struct {
	eventCount uint64
	countLock sync.Mutex
}


// implements pc.ComponentEventListener
func (kvsc *KVFeedStatisticsCollector) OnEvent (eventType pc.ComponentEventType, item interface{}, component pc.Component, derivedItems []interface{}, otherInfos map[string]interface{}) {
	kvsc.countLock.Lock()
	defer kvsc.countLock.Unlock()
	
	c.Debugf("KVFeedStatisticsCollector OnEvent called on collector %v for eventType %v \n", kvsc, eventType)
	switch (eventType) {
		case pc.DataProcessed:
			kvsc.eventCount ++;
		default:
	}
}

func (kvsc *KVFeedStatisticsCollector) Statistics() uint64 {
	kvsc.countLock.Lock()
	defer kvsc.countLock.Unlock()
	
	return kvsc.eventCount
}

