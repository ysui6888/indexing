// Connector connecting KVFeed to downstream nodes, i.e., KVFeed

package projector

import (
	pc "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
)

// Collector of KVFeed statistics
type KVFeedStatisticsCollector struct {
	eventCount uint64
}


// implements pc.PartEventListener
func (kvsc *KVFeedStatisticsCollector) OnEvent (eventType pc.PartEventType, item interface{}, part pc.Part, derivedItems []interface{}, otherInfos map[string]interface{}) {
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
	return kvsc.eventCount
}

