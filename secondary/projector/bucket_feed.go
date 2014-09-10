// concurrency model:
//
//                                NewBucketFeed()
//                                     |
//                                  (spawn)
//                                     |
//    RequestFeed() -----*-*-*-----> genServer() --*----> KVFeed
//              |        | | |                     |
//  <--failTs,kvTs       | | |                     *----> KVFeed
//                       | | |                     |
//     CloseFeed() ------* | |                     *----> KVFeed
//                         | |
//   UpdateEngines() ------* |
//                           |
//   DeleteEngines() --------*
//
// Notes:
//
// - new bucket-feed spawns a gen-server routine for control path.
// - RequestFeed can start, restart or shutdown one or more vbuckets across
//   kv-nodes.
// - for a successful RequestFeed,
//   - failover-timestamp, restart-timestamp from different kv-nodes
//     (containing an exlusive set of vbuckets) will be aggregated into a
//     single failover-timestamp and restart-timestamp and return back.
//   - if request is to shutdown vbuckets, then failover-timetamp and
//     restart-timetamp will be empty.
//
// TODO
//  - make `kvfeeds` mutable, kvnodes can be added or removed from a live
//    feed.

package projector

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	pc "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
)

// BucketFeed is per bucket, multiple kv-node feeds, for a subset of vbuckets.
type BucketFeed struct {
	// Part
	pp.AbstractPart  
	// immutable fields
	feed    *Feed
	pooln   string
	kvfeeds map[string]*KVFeed // kvaddr -> *KVFeed
	kvfeedStatsCollectors map[string]*KVFeedStatisticsCollector  // kvaddr -> *KVFeedStatisticsCollector
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// misc.
	logPrefix string
	stats     c.Statistics
	// indicates whether the BucketFeed has been started
	started  bool  
}

// NewBucketFeed creates a new instance of feed for specified bucket. 
//
// if error, BucketFeed is not created.
// - error returned by couchbase client, via NewKVFeed()
func NewBucketFeed(
	feed *Feed,
	kvaddrs []string, // if co-located, len(kvaddrs) equals 1
	pooln, bucketn string) (bfeed *BucketFeed, err error) {

	bfeed = &BucketFeed{
		feed:    feed,
		pooln:   pooln,
		kvfeeds: make(map[string]*KVFeed),
		kvfeedStatsCollectors: make(map[string]*KVFeedStatisticsCollector),
		reqch:   make(chan []interface{}, c.GenserverChannelSize),
		finch:   make(chan bool),
	}
	
	// uses bucket name as the part Id  for BucketFeed
	var isStarted_callback_func pp.IsStarted_Callback_Func = bfeed.IsStarted
	bfeed.AbstractPart = pp.NewAbstractPart(bucketn, &isStarted_callback_func)
	
	bfeed.logPrefix = fmt.Sprintf("[%v]", bfeed.repr())
	bfeed.stats = bfeed.newStats()

	p := bfeed.getFeed().getProjector()
	bucket, err := p.getBucket(pooln, bucketn)
	if err != nil {
		c.Errorf("%v getBucket(): %v\n", bfeed.logPrefix, err)
		return nil, err
	}
	
	// initialize KVFeeds
	for _, kvaddr := range kvaddrs {		
		kvfeed, err := NewKVFeed(kvaddr, bfeed.repr(), bucket)
		if err != nil {
			bfeed.doClose()
			return nil, err
		}
		// set the connector of KVFeed to KVFeedConnector to enable it to work with VbucketRoutines
		connector, err := NewKVFeedConnector(bucketn, kvfeed.repr())
		if err != nil {
			bfeed.doClose()
			return nil, err
		}
		kvfeed.SetConnector(connector)
		
		// register KVFeedStatisticsCollector as an event listener for KVFeed
		kvfeedStatsCollector := new(KVFeedStatisticsCollector)
		kvfeed.RegisterPartEventListener(pc.DataProcessed, kvfeedStatsCollector)
		bfeed.kvfeedStatsCollectors[kvaddr] = kvfeedStatsCollector
		
		bfeed.kvfeeds[kvaddr] = kvfeed
	}
	c.Infof("%v bfeed created ...\n", bfeed.logPrefix)
	return bfeed, nil
}

func (bfeed *BucketFeed) repr() string {
	return fmt.Sprintf("%v:%v", bfeed.feed.repr(), bfeed.Id())
}

func (bfeed *BucketFeed) getFeed() *Feed {
	return bfeed.feed
}

// gen-server API commands
const (
	bfCmdRequestFeed byte = iota + 1
	bfCmdUpdateEngines
	bfCmdDeleteEngines
	bfCmdGetStatistics
	bfCmdCloseFeed
)

// RequestFeed synchronous call.
//
// returns failover-timetamp and kv-timestamp
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if BucketFeed is already closed.
func (bfeed *BucketFeed) RequestFeed(
	request RequestReader,
	endpoints map[string]*Endpoint,
	engines map[uint64]*Engine) error {

	if request == nil || engines == nil || len(engines) == 0 {
		return ErrorArgument
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdRequestFeed, request, endpoints, engines, respch}
	resp, err := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	return c.OpError(err, resp, 0)
}

// UpdateFeed synchronous call.
//
// returns failover-timetamp and kv-timestamp
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if BucketFeed is already closed.
func (bfeed *BucketFeed) UpdateFeed(
	request RequestReader,
	endpoints map[string]*Endpoint,
	engines map[uint64]*Engine) error {

	if request == nil || engines == nil || len(engines) == 0 {
		return ErrorArgument
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdRequestFeed, request, endpoints, engines, respch}
	resp, err := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	return c.OpError(err, resp, 0)
}

// UpdateEngines synchronous call.
//
// - error if BucketFeed is already closed.
func (bfeed *BucketFeed) UpdateEngines(endpoints map[string]*Endpoint, engines map[uint64]*Engine) error {
	if engines == nil || len(engines) == 0 {
		return ErrorArgument
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdUpdateEngines, endpoints, engines, respch}
	resp, err := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	return c.OpError(err, resp, 0)
}

// DeleteEngines synchronous call.
//
// - error if BucketFeed is already closed.
func (bfeed *BucketFeed) DeleteEngines(endpoints map[string]*Endpoint, engines []uint64) error {
	if engines == nil || len(engines) == 0 {
		return ErrorArgument
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdDeleteEngines, endpoints, engines, respch}
	resp, err := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	return c.OpError(err, resp, 0)
}

// GetStatistics will recursively get statistics for bucket-feed and its
// underlying workers.
func (bfeed *BucketFeed) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	return resp[0].(map[string]interface{})
}

// CloseFeed synchronous call.
//
// - error if BucketFeed is already closed.
func (bfeed *BucketFeed) CloseFeed() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdCloseFeed, respch}
	resp, err := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	return c.OpError(err, resp, 0)
}

// routine handles control path.
func (bfeed *BucketFeed) genServer(reqch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			c.Errorf("%v paniced: %v !\n", bfeed.logPrefix, r)
			bfeed.doClose()
		}
	}()

loop:
	for {
		msg := <-reqch
		switch msg[0].(byte) {
		case bfCmdRequestFeed:
			req := msg[1].(RequestReader)
			endpoints := msg[2].(map[string]*Endpoint)
			engines := msg[3].(map[uint64]*Engine)
			respch := msg[4].(chan []interface{})
			err := bfeed.requestFeed(req, endpoints, engines)
			respch <- []interface{}{err}

		case bfCmdUpdateEngines:
			endpoints := msg[1].(map[string]*Endpoint)
			engines := msg[2].(map[uint64]*Engine)
			respch := msg[3].(chan []interface{})
			bfeed.updateEngines(endpoints, engines)
			respch <- []interface{}{nil}

		case bfCmdDeleteEngines:
			endpoints := msg[1].(map[string]*Endpoint)
			engines := msg[2].([]uint64)
			respch := msg[3].(chan []interface{})
			// call DeleteEngines on all downstream KVFeedConnectors
			for _, kvfeed := range bfeed.kvfeeds {
				kvfeed.Connector().(*KVFeedConnector).DeleteEngines(endpoints, engines)	
			}
			respch <- []interface{}{nil}

		case bfCmdGetStatistics:
			respch := msg[1].(chan []interface{})
			kvfeeds, _ := c.NewStatistics(bfeed.stats.Get("kvfeeds"))
				for kvaddr, kvfeed := range bfeed.kvfeeds {
				kvfeedStats := make(map[string]interface{})
				// add statistics from KVFeed
				kvfeedStats["events"] = bfeed.kvfeedStatsCollectors[kvaddr].Statistics()
				for partId, part := range kvfeed.Connector().DownStreams() {
					// add statistics from downstream VBucketRountines
					s := fmt.Sprintf("%v", partId)
					kvfeedStats[s] = part.(*VbucketRoutine).GetStatistics()
				}
				kvfeeds.Set(kvaddr, kvfeedStats)
			}
			bfeed.stats.Set("kvfeeds", kvfeeds)
			respch <- []interface{}{bfeed.stats.ToMap()}

		case bfCmdCloseFeed:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{bfeed.doClose()}
			break loop
		}
	}
}

// request a new feed or start, restart and shutdown upstream vbuckets and/or
// update downstream engines.
func (bfeed *BucketFeed) requestFeed(
	req RequestReader,
	endpoints map[string]*Endpoint,
	engines map[uint64]*Engine) error {

	c.Debugf("%v updating feed ...", bfeed.logPrefix)

	bfeed.updateEngines(endpoints, engines)
	
	for _, kvfeed := range bfeed.kvfeeds {
		// have to call requestFeed instead of RequestFeed here since kvfeed gen-server may not have been started 
		err := kvfeed.requestFeed(req)
		if err != nil {
			return err
		}
	}

	return nil
}

// update Engines in downstream nodes
func(bfeed *BucketFeed) updateEngines(
	endpoints map[string]*Endpoint,
	engines map[uint64]*Engine){
		
		// call UpdateEngines on all downstream KVFeedConnectors
		for _, kvfeed := range bfeed.kvfeeds {
			kvfeed.Connector().(*KVFeedConnector).UpdateEngines(endpoints, engines)	
		}
}

// execute close
func (bfeed *BucketFeed) doClose() (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v doClose() paniced: %v !\n", bfeed.logPrefix, r)
		}
	}()

	// proceed to close the kvfeed
	for _, kvfeed := range bfeed.kvfeeds {
	    // KVFeed may not have been started at this point, in which case there is no need to stop it
		if kvfeed.IsStarted() {
			kvfeed.Stop()
			// moved the stopping of downstream VbucketRoutines from KVFeed to here since VbucketRoutines do not 
			// need to stopped each time KVFeed is.
			for _, vr := range kvfeed.Connector().DownStreams(){
				vr.(*VbucketRoutine).Stop()
			}
		}
	}
	
	// close the gen-server
	close(bfeed.finch)
	bfeed.kvfeeds = nil
	bfeed.kvfeedStatsCollectors = nil
	c.Infof("%v ... stopped\n", bfeed.logPrefix)
	return
}

// implements Part
func (bfeed *BucketFeed) Start(settings map[string]interface{}) error {
	go bfeed.genServer(bfeed.reqch)
	c.Infof("%v started ...\n", bfeed.logPrefix)
	
	bfeed.started = true
	return nil
}

func (bfeed *BucketFeed) Stop() error {
	err := bfeed.CloseFeed() 
	if err == nil {
		bfeed.started = false
	} 
	return err
}

func (bfeed *BucketFeed) Receive (data interface{}) error {
	// BucketFeed is a source nozzle and does not receive from upstream nodes
	return nil
}

func (bfeed *BucketFeed) IsStarted() bool {
	return bfeed.started
}

// implements Nozzle
// These methods are not actively used and are not implemented.
func (bfeed *BucketFeed) Open() error {
	return nil
}

func (bfeed *BucketFeed) Close() error {
	return nil
}

func (bfeed *BucketFeed) IsOpen() bool {
	return false
}
