// concurrency model:
//
//                                           V0 V1    Vn
//                                            ^  ^ ..  ^
//                                            |  |     |
//                                      Begin |  |     |
//                                   Mutation |  |     |
//                                   Deletion |  |     |
//                                       Sync |  |     |
//                      NewKVFeed()       End |  |     |
//                           |  |             |  |     |
//                          (spawn)         ** UprEvent **
//                           |  |             |
//                           |  *----------- runScatter()
//                           |                      ^
//                           |                      |
//    RequestFeed() -----*-> genServer()            *-- vbucket stream
//                       |      ^                   |
//                       |      |                   *-- vbucket stream
//                       |      |                   |
//    CloseFeed() -------*      |                   *-- vbucket stream
//                              |                   |
//                              *------------> couchbase-client
//
// Notes:
//
// - new kv-feed spawns a gen-server routine for control path and
//   gather-scatter routine for data path.
// - RequestFeed can start, restart or shutdown one or more vbuckets.
// - for a successful RequestFeed,
//   - failover-timestamp, restart-timestamp must contain timestamp for
//     "active vbuckets".
//   - if request is to shutdown vbuckets, failover-timetamp and
//     restart-timetamp will be empty.
//   - StreamBegin and StreamEnd events are gauranteed by couchbase-client.
// - for idle vbuckets periodic Sync events will be published downstream.
// - KVFeed will be closed, notifying downstream component with,
//   - nil, when downstream component does CloseFeed()
//   - ErrorClientExited, when upstream closes the mutation channel
//   - ErrorShiftingVbucket, when vbuckets are shifting

package projector

import (
	"errors"
	"fmt"
	"sync"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/go-couchbase"
	pc "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
)

// error codes

// ErrorVBmap
var ErrorVBmap = errors.New("kvfeed.vbmap")

// ErrorClientExited
var ErrorClientExited = errors.New("kvfeed.clientExited")

// ErrorShiftingVbucket
var ErrorShiftingVbucket = errors.New("kvfeed.shiftingVbucket")

// ErrorInvalidStartSettingsForKVFeed
var ErrorInvalidStartSettingsForKVFeed = errors.New("kvfeed.invalidStartSettingsForKVFeed")

// KVFeed is per bucket, per node feed for a subset of vbuckets
type KVFeed struct {
	// Part
	pp.AbstractPart 
	// immutable fields
	bucket  BucketAccess
	feeder  KVFeeder
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// misc.
	// repr of parent/calling module
	parentRepr string   
	logPrefix string
	// indicates whether the KVFeed has been started
	started  bool  
	done sync.WaitGroup //makes KVFeed wait on the two go routines it spawns before it can declare itself stopped
}

// NewKVFeed create a new feed from `kvaddr` node for a single bucket. Uses
// couchbase client API to identify the subset of vbuckets mapped to this
// node.
//
// if error, KVFeed is not started
// - error returned by couchbase client
func NewKVFeed(kvaddr, parentRepr string, bucket *couchbase.Bucket) (*KVFeed, error) {
	kvfeed := &KVFeed{
		bucket: bucket,
		parentRepr: parentRepr,
	}
	
	// uses kvaddr as the part Id  for KVFeed
	kvfeed.AbstractPart = pp.NewAbstractPart(kvaddr)
	
	kvfeed.logPrefix = fmt.Sprintf("[%v]", kvfeed.repr())
	
	c.Infof("%v kvfeed created ...\n", kvfeed.logPrefix)
	
	return kvfeed, nil
}

func (kvfeed *KVFeed) repr() string {
	return fmt.Sprintf("%v:%v", kvfeed.parentRepr, kvfeed.Id())
}

// APIs to gen-server
const (
	kvfCmdRequestFeed byte = iota + 1
	kvfCmdCloseFeed
)

// RequestFeed synchronous call.
//
// returns failover-timetamp and kv-timestamp.
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if KVFeed is already closed.
func (kvfeed *KVFeed) RequestFeed(
	req RequestReader) error {
	if req == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvfCmdRequestFeed, req, respch}
	resp, err := c.FailsafeOp(kvfeed.reqch, respch, cmd, kvfeed.finch)
	return c.OpError(err, resp, 0)
}

// CloseFeed synchronous call.
func (kvfeed *KVFeed) CloseFeed() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvfCmdCloseFeed, respch}
	resp, err := c.FailsafeOp(kvfeed.reqch, respch, cmd, kvfeed.finch)
	return c.OpError(err, resp, 0)
}

// routine handles control path.
func (kvfeed *KVFeed) genServer(reqch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			c.Errorf("%v paniced: %v !\n", kvfeed.logPrefix, r)
			kvfeed.doClose()
		}
	}()

loop:
	for {
		msg := <-reqch
		switch msg[0].(byte) {
		case kvfCmdRequestFeed:
			req := msg[1].(RequestReader)
			respch := msg[2].(chan []interface{})
			err := kvfeed.requestFeed(req)
			respch <- []interface{}{err}

		case kvfCmdCloseFeed:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{kvfeed.doClose()}
			break loop
		}
	}
	
	kvfeed.done.Done()
}

// start, restart or shutdown streams
func (kvfeed *KVFeed) requestFeed(req RequestReader) error {
	prefix := kvfeed.logPrefix
	
	c.Debugf("%v updating feed ...", prefix)

	// fetch restart-timestamp from request
	ts := req.RestartTimestamp(kvfeed.bucket.(*couchbase.Bucket).Name)
	if ts == nil {
		c.Errorf("%v restartTimestamp is empty\n", prefix)
		return c.ErrorInvalidRequest
	}

	// refresh vbmap before fetching it.
	if err := kvfeed.bucket.Refresh(); err != nil {
		c.Errorf("%v bucket.Refresh() %v \n", prefix, err)
	}

	m, err := kvfeed.bucket.GetVBmap([]string{kvfeed.Id()})
	if err != nil {
		c.Errorf("%v bucket.GetVBmap() %v \n", prefix, err)
		return err
	}
	vbnos := m[kvfeed.Id()]
	if vbnos == nil {
		return ErrorVBmap
	}

	// filter vbuckets for this kvfeed.
	ts = ts.SelectByVbuckets(vbnos)
	
	settings := ConstructStartSettingsForKVFeed(ts)

	c.Debugf("start: %v restart: %v shutdown: %v\n",
		req.IsStart(), req.IsRestart(), req.IsShutdown())

	// execute the request
	if req.IsStart() { // start
		err = kvfeed.Start(settings)

	} else if req.IsRestart() { // restart implies a shutdown and start
		if err = kvfeed.Stop(); err == nil {
			// TODO may need to compute restart timestamp based on shutdown timestamp instead of directly using the latter  
			err = kvfeed.Start(settings)
		}

	} else if req.IsShutdown() { // shutdown
		err = kvfeed.Stop()
		// TODO may need to restart kvfeed if the vbtreams that were shutdown are different from the vbstreams that were running.
	} else {
		err = c.ErrorInvalidRequest
		c.Errorf("%v %v", prefix, err)
	}
	return err
}

// execute close.
func (kvfeed *KVFeed) doClose() error {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v doClose() paniced: %v !\n", kvfeed.logPrefix, r)
		}
	}()

	// closing KVFeed will not stop downstream VbucketRoutines, which will be stopped by BucketFeed when BucketFeed is stopped
	
	// close upstream
	kvfeed.feeder.CloseKVFeed()
	close(kvfeed.finch)
	c.Infof("%v ... stopped\n", kvfeed.logPrefix)
	return nil
}

// routine handles data path.
func (kvfeed *KVFeed) runScatter() {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v runScatter() panic: %v\n", kvfeed.logPrefix, r)
		}
	}()

	mutch := kvfeed.feeder.GetChannel()
	finch := kvfeed.finch
	
loop:
	for {
		select {
			case m, ok := <-mutch: // mutation from upstream
				if ok == false {
					kvfeed.CloseFeed()
					break loop
				}
				c.Tracef("%v, Mutation %v:%v:%v <%v>\n",
					kvfeed.logPrefix, m.VBucket, m.Seqno, m.Opcode, m.Key)
				// forward mutation downstream through connector
				if err := kvfeed.Connector().Forward(m); err != nil {
					c.Errorf("%v error forwarding uprEvent for vbucket(%v) %v", kvfeed.logPrefix, m.VBucket, err)
				}
				// raise event for statistics collection
				kvfeed.RaiseEvent(pc.DataProcessed, nil/*item*/, kvfeed, nil/*derivedItems*/, nil/*otherInfos*/)
			// stop runScatter() if finch is closed, which indicates that the KVFeed is being stopped
			case <-finch:
				break loop  
		}
	}
	
	kvfeed.done.Done()
}

// construct start settings for KVFeed, which contains a single restart timestamp
func ConstructStartSettingsForKVFeed(ts *protobuf.TsVbuuid) map[string]interface{} {
		settings := make(map[string]interface{})
		// The "Key" key is never used and carries no significance
		settings["Key"] = ts
		
	return settings
}

// parse out restart timestamp from start settings
func parseStartSettingsForKVFeed(settings map[string]interface{}) (*protobuf.TsVbuuid, error) {
	var ts *protobuf.TsVbuuid
	var ok bool 
	if len(settings) != 1 {
		return nil, ErrorInvalidStartSettingsForKVFeed
	}
	for _, setting := range settings {
		if ts, ok = setting.(*protobuf.TsVbuuid); !ok {
			return nil, ErrorInvalidStartingSettingsForFeed
		} 
	}
	
	return ts, nil
}

// implements Part

// start KVFeed by starting VB stream on feeder
func(kvfeed *KVFeed) Start(settings map[string]interface{}) error {
	// initializes feeder in KVFeed
	feeder, err := OpenKVFeed(kvfeed.bucket.(*couchbase.Bucket), kvfeed.Id(), kvfeed)
	if err != nil {
		c.Errorf("%v OpenKVFeed(): %v\n", kvfeed.logPrefix, err)
		return err
	}
	kvfeed.feeder = feeder.(KVFeeder)
	
	// initializes channels
	kvfeed.reqch = make(chan []interface{}, c.GenserverChannelSize)
	kvfeed.finch = make(chan bool)
	
	
	// parse start settings 
	ts, err := parseStartSettingsForKVFeed(settings);
	if  err != nil {
		return err
	}
	
	flogs, err := kvfeed.bucket.GetFailoverLogs(c.Vbno32to16(ts.Vbnos))
	if err != nil {
		return err
	}
	
	go kvfeed.genServer(kvfeed.reqch)
	go kvfeed.runScatter()
	
	c.Debugf("%v start-timestamp %#v\n", kvfeed.logPrefix, ts)
	if _, _, err = kvfeed.feeder.StartVbStreams(flogs, ts); err != nil {
		c.Errorf("%v feeder.StartVbStreams() %v", kvfeed.logPrefix, err)
	}

	kvfeed.done.Add(2)  // waiting for two go rountines, gen-server and runScatter 
	kvfeed.started = true
	c.Infof("%v started ...\n", kvfeed.logPrefix)
	return err
}

func(kvfeed *KVFeed) Stop() error {
	err := kvfeed.CloseFeed()
	if err == nil {
		kvfeed.started = false
		kvfeed.done.Wait() // wait for both go rountines, gen-server and runScatter, to stop
	}
	return err	
}

func (kvfeed *KVFeed) Receive (data interface{}) error {
	// KVFeed is a source nozzle and does not receive from upstream nodes
	return nil
}

func (kvfeed *KVFeed) IsStarted() bool {
	return kvfeed.started
}

// implements Nozzle
// methods not actively used and not implemented
func(kvfeed *KVFeed) Open() error {
	return nil
}

func(kvfeed *KVFeed) Close() error {
	return nil
}

func(kvfeed *KVFeed) IsOpen() bool {
	return false
}
