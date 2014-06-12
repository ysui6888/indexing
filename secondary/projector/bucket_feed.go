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
	"log"
	"sort"
)

// BucketFeed is per bucket, multiple kv-node feeds, for a subset of vbuckets.
type BucketFeed struct {
	// immutable fields
	feed    *Feed
	bucketn string
	pooln   string
	kvfeeds map[string]*KVFeed // kvaddr -> *KVFeed
	// gen-server
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
}

// NewBucketFeed creates a new instance of feed for specified bucket. Spawns a
// routine for gen-server.
//
// if error, BucketFeed is not created.
// - error returned by couchbase client, via NewKVFeed()
func NewBucketFeed(
	feed *Feed,
	kvaddrs []string, // if co-located, len(kvaddrs) equals 1
	pooln, bucketn string) (bfeed *BucketFeed, err error) {

	bfeed = &BucketFeed{
		feed:      feed,
		bucketn:   bucketn,
		pooln:     pooln,
		kvfeeds:   make(map[string]*KVFeed),
		reqch:     make(chan []interface{}, c.GenserverChannelSize),
		finch:     make(chan bool),
		logPrefix: fmt.Sprintf("bucket-feed %v:%v", feed.topic, bucketn),
	}

	// initialize KVFeeds
	for _, kvaddr := range kvaddrs {
		kvfeed, err := NewKVFeed(bfeed, kvaddr, pooln, bucketn)
		if err != nil {
			bfeed.doClose()
			return nil, err
		}
		bfeed.kvfeeds[kvaddr] = kvfeed
	}
	go bfeed.genServer(bfeed.reqch)
	log.Printf("%v, started ...\n", bfeed.logPrefix)
	return bfeed, nil
}

func (bfeed *BucketFeed) getFeed() *Feed {
	return bfeed.feed
}

// gen-server API commands
const (
	bfCmdRequestFeed byte = iota + 1
	bfCmdUpdateEngines
	bfCmdDeleteEngines
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
	engines map[uint64]*Engine) (*c.Timestamp, *c.Timestamp, error) {

	if request == nil || engines == nil || len(engines) == 0 {
		return nil, nil, ErrorArgument
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdRequestFeed, request, endpoints, engines, respch}
	resp, err := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	if err = c.OpError(err, resp, 2); err != nil {
		return nil, nil, err
	}
	failoverTs, kvTs := resp[0].(*c.Timestamp), resp[1].(*c.Timestamp)
	return failoverTs, kvTs, nil
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
	engines map[uint64]*Engine) (failoverTs, kvTs *c.Timestamp, err error) {

	if request == nil || engines == nil || len(engines) == 0 {
		return nil, nil, ErrorArgument
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{bfCmdRequestFeed, request, endpoints, engines, respch}
	resp, err := c.FailsafeOp(bfeed.reqch, respch, cmd, bfeed.finch)
	if err = c.OpError(err, resp, 2); err != nil {
		return nil, nil, err
	}
	failoverTs, kvTs = resp[0].(*c.Timestamp), resp[1].(*c.Timestamp)
	return failoverTs, kvTs, nil
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
			log.Printf("%v, paniced: %v\n", bfeed.logPrefix, r)
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
			failTs, kvTs, err := bfeed.requestFeed(req, endpoints, engines)
			respch <- []interface{}{failTs, kvTs, err}

		case bfCmdUpdateEngines:
			endpoints := msg[1].(map[string]*Endpoint)
			engines := msg[2].(map[uint64]*Engine)
			respch := msg[3].(chan []interface{})
			for _, kvfeed := range bfeed.kvfeeds {
				kvfeed.UpdateEngines(endpoints, engines)
			}
			respch <- []interface{}{nil}

		case bfCmdDeleteEngines:
			endpoints := msg[1].(map[string]*Endpoint)
			engines := msg[2].([]uint64)
			respch := msg[3].(chan []interface{})
			for _, kvfeed := range bfeed.kvfeeds {
				kvfeed.DeleteEngines(endpoints, engines)
			}
			respch <- []interface{}{nil}

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
	engines map[uint64]*Engine) (failTs, kvTs *c.Timestamp, err error) {

	var fTs, vTs *c.Timestamp
	failTs = c.NewTimestamp(bfeed.bucketn, c.MaxVbuckets)
	kvTs = c.NewTimestamp(bfeed.bucketn, c.MaxVbuckets)
	for _, kvfeed := range bfeed.kvfeeds {
		fTs, vTs, err = kvfeed.RequestFeed(req, endpoints, engines)
		if err != nil {
			return nil, nil, err
		}
		failTs = failTs.Union(fTs)
		kvTs = kvTs.Union(vTs)
	}
	sort.Sort(failTs)
	sort.Sort(kvTs)
	return failTs, kvTs, nil
}

// execute close
func (bfeed *BucketFeed) doClose() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%v, paniced: %v\n", bfeed.logPrefix, r)
		}
	}()

	// proceed closing the upstream
	for _, kvfeed := range bfeed.kvfeeds {
		kvfeed.CloseFeed()
	}
	// close the gen-server
	close(bfeed.finch)
	bfeed.kvfeeds = nil
	log.Printf("%v, ... closed\n", bfeed.logPrefix)
	return
}