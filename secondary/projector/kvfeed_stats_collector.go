// Connector connecting KVFeed to downstream nodes, i.e., KVFeed

package projector

import (
    "sync"
	pc "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	c "github.com/couchbase/indexing/secondary/common"
)

// Collector of KVFeed statistics
type KVFeedStatisticsCollector struct {
	eventCount uint64
	countLock sync.Mutex
}


// implements pc.PartEventListener
func (kvsc *KVFeedStatisticsCollector) OnEvent (eventType pc.PartEventType, item interface{}, part pc.Part, derivedItems []interface{}, otherInfos map[string]interface{}) {
	kvsc.countLock.Lock()
	defer kvsc.countLock.Unlock()
	
	c.Infof("KVFeedStatisticsCollector OnEvent called on collector %v for eventType %v \n", kvsc, eventType)
	if _, ok := part.(*KVFeed); ok {
		switch (eventType) {
			case pc.DataProcessed:
				kvsc.eventCount ++;
			default:
		}
	}
	
	// Question: do we throw error when part is not KVFeed? it should not happen with the way OnEvent is called
			
}

func (kvsc *KVFeedStatisticsCollector) Statistics() uint64 {
	kvsc.countLock.Lock()
	defer kvsc.countLock.Unlock()
	return kvsc.eventCount
}

