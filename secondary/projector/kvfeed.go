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
	pc "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/go-couchbase"
	"runtime"
	"sync"
	"time"
)

// error codes

// ErrorVBmap
var ErrorVBmap = errors.New("kvfeed.vbmap")

// ErrorVBList
var ErrorEmptyVBList = errors.New("kvfeed.emptyVBList")

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
	// the list of vbuckets that the kvfeed is responsible for
	// this allows multiple kvfeeds to be created for a kv node
	vbnos []uint16
	// immutable fields
	kvaddr   string
	bucket   BucketAccess
	feeder   KVFeeder
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// misc.
	// repr of parent/calling module
	parentRepr string
	logPrefix  string
	// indicates whether the KVFeed has been started
	started  bool  
	// RW lock for started flag
	startLock sync.RWMutex
	done sync.WaitGroup //makes KVFeed wait on the two go routines it spawns before it can declare itself stopped
	stats     c.Statistics
	counter	int
	start_time time.Time
}

// NewKVFeed create a new feed from `kvaddr` node for a single bucket. Uses
// couchbase client API to identify the subset of vbuckets mapped to this
// node.
//
// if error, KVFeed is not started
// - error returned by couchbase client
func NewKVFeed(kvaddr, parentRepr, partId string, bucket *couchbase.Bucket, vbnos []uint16) (*KVFeed, error) {
	kvfeed := &KVFeed{
		vbnos:      vbnos,
		kvaddr:     kvaddr,
		bucket:     bucket,
		parentRepr: parentRepr,
		counter: 0,
    }
	kvfeed.logPrefix = fmt.Sprintf("[%v]", kvfeed.repr())

	// uses kvaddr as the part Id  for KVFeed
	var isStarted_callback_func pp.IsStarted_Callback_Func = kvfeed.IsStarted
	var kvfeedId string
	if len(partId) > 0 {
		kvfeedId = partId
	} else {
		// default the id of kvfeed to kvaddr if not specified
		kvfeedId = kvaddr
	}
	kvfeed.AbstractPart = pp.NewAbstractPart(kvfeedId, &isStarted_callback_func)

	if kvfeed.vbnos == nil {
		// if vbnos is not specified, default it to all vbuckets in the kv node
		if err := kvfeed.setVBListFromBucket(); err != nil {
			return nil, err
		}
	}

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
	if err != nil {
		c.Errorf("CloseFeed failed: err=%v\n", err)
	}

	close(kvfeed.finch)
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
	var err error
	prefix := kvfeed.logPrefix
	
	c.Debugf("%v requestFeed ...", prefix)

	// fetch restart-timestamp from request
	ts := req.RestartTimestamp(kvfeed.bucket.(*couchbase.Bucket).Name)
	if ts == nil {
		c.Errorf("%v restartTimestamp is empty\n", prefix)
		return c.ErrorInvalidRequest
	}

	// update vb list in kvfeed using the up-to-date vb list from bucket
	if err := kvfeed.setVBListFromBucket(); err != nil {
		return err
	}

	// filter vbuckets for this kvfeed.
	ts = ts.SelectByVbuckets(kvfeed.vbnos)

	settings := ConstructStartSettingsForKVFeed(ts)

	c.Debugf("start: %v restart: %v shutdown: %v\n",
		req.IsStart(), req.IsRestart(), req.IsShutdown())

	// execute the request
	if req.IsStart() { // start
		err = kvfeed.Start(settings)

	} else if req.IsRestart() { // restart implies a shutdown and start
		// kvfeed may be new and not started. If so there is no need to stop it
		if kvfeed.IsStarted() {
			if err = kvfeed.Stop(); err != nil {
				return err
			}
		}
		// TODO may need to compute restart timestamp based on shutdown timestamp instead of directly using the latter  
		err = kvfeed.Start(settings)

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
	c.Infof("%v ... stopped\n", kvfeed.logPrefix)
	trace := make([]byte, 1024)
	count := runtime.Stack(trace, true)
	c.Infof("Stack of %d bytes: %s", count, trace)

	return nil
}

// routine handles data path.
func (kvfeed *KVFeed) runScatter() {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v runScatter() panic: %v\n", kvfeed.logPrefix, r)
		}
		kvfeed.done.Done()
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
			kvfeed.counter ++
			c.Tracef("%v, Mutation %v:%v:%v <%v>, counter=%v, ops_per_sec=%v\n",
				kvfeed.logPrefix, m.VBucket, m.Seqno, m.Opcode, m.Key, kvfeed.counter, float64(kvfeed.counter)/time.Since(kvfeed.start_time).Seconds())
			c.Debugf("%v ops_per_sec=%v\n",
				kvfeed.Id(), float64(kvfeed.counter)/time.Since(kvfeed.start_time).Seconds())

			// forward mutation downstream through connector
			if err := kvfeed.Connector().Forward(m); err != nil {
				c.Errorf("%v error forwarding uprEvent for vbucket(%v) %v", kvfeed.logPrefix, m.VBucket, err)
			}
			// raise event for statistics collection
			kvfeed.RaiseEvent(pc.DataProcessed, nil /*item*/, kvfeed, nil /*derivedItems*/, nil /*otherInfos*/)
		// stop runScatter() if finch is closed, which indicates that the KVFeed is being stopped
		case <-finch:
			break loop
		}
	}

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

	c.Debugf("length of the settings is %d\n", len(settings))
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
	kvfeed.startLock.Lock()
	defer kvfeed.startLock.Unlock()

	// initializes feeder in KVFeed
	c.Debugf("%v KVFeed Start....\n", kvfeed.logPrefix)
	// parse start settings

	feeder, err := OpenKVFeed(kvfeed.bucket.(*couchbase.Bucket), kvfeed.kvaddr, kvfeed)
	if err != nil {
		c.Errorf("%v OpenKVFeed(): %v, id=%v, vbnos=%v\n", kvfeed.logPrefix, err, kvfeed.Id(), kvfeed.vbnos)
		return err
	}
	kvfeed.feeder = feeder.(KVFeeder)

	// initializes channels
	kvfeed.reqch = make(chan []interface{}, c.GenserverChannelSize)
	kvfeed.finch = make(chan bool)

	ts, err := parseStartSettingsForKVFeed(settings)
	if err != nil {
		c.Errorf("Failed to parse setting for KVFeed. err=%v\n", err)
		return err
	}
	c.Debugf("%v KVFeed Start - finish parsing settings\n", kvfeed.logPrefix)

//	flogs, err := kvfeed.bucket.GetFailoverLogs(c.Vbno32to16(ts.Vbnos))
//	if err != nil {
//		return err
//	}

	kvfeed.start_time = time.Now()
	
	go kvfeed.genServer(kvfeed.reqch)
	go kvfeed.runScatter()

	c.Debugf("%v start-timestamp %#v\n", kvfeed.logPrefix, ts)
	if _, _, err = kvfeed.feeder.StartVbStreams(nil, ts); err != nil {
		c.Errorf("%v feeder.StartVbStreams() %v", kvfeed.logPrefix, err)
	}

	kvfeed.done.Add(2) // waiting for two go rountines, gen-server and runScatter
	kvfeed.started = true
	c.Infof("%v started ...\n", kvfeed.logPrefix)
	return err
}

func(kvfeed *KVFeed) Stop() error {
	kvfeed.startLock.Lock()
	defer kvfeed.startLock.Unlock()

	err := kvfeed.CloseFeed()
	if err == nil {
		kvfeed.started = false
		kvfeed.done.Wait() // wait for both go rountines, gen-server and runScatter, to stop
	}

	c.Infof("%v stopped ...\n", kvfeed.logPrefix)
	return err	
}

func (kvfeed *KVFeed) Receive(data interface{}) error {
	// KVFeed is a source nozzle and does not receive from upstream nodes
	return nil
}

func (kvfeed *KVFeed) IsStarted() bool {
	kvfeed.startLock.RLock()
	defer kvfeed.startLock.RUnlock()
	
	return kvfeed.started
}

// implements Nozzle
// methods not actively used and not implemented
func (kvfeed *KVFeed) Open() error {
	return nil
}

func (kvfeed *KVFeed) Close() error {
	return nil
}

func (kvfeed *KVFeed) IsOpen() bool {
	return false
}

// Set vb list in kvfeed
func (kvfeed *KVFeed) SetVBList(vbnos []uint16) error {
	if len(vbnos) == 0 {
		return ErrorEmptyVBList
	}
	kvfeed.vbnos = vbnos
	return nil
}

func (kvfeed *KVFeed) GetVBList() []uint16 {
	return kvfeed.vbnos
}

// set vb list of kvfeed to that in bucket
func (kvfeed *KVFeed) setVBListFromBucket() error {
	// refresh vbmap before fetching it.
	if err := kvfeed.bucket.Refresh(); err != nil {
		c.Errorf("%v bucket.Refresh() %v \n", kvfeed.logPrefix, err)
	}

	m, err := kvfeed.bucket.GetVBmap([]string{kvfeed.kvaddr})
	if err != nil {
		c.Errorf("%v bucket.GetVBmap() %v \n", kvfeed.logPrefix, err)
		return err
	}
	kvfeed.vbnos = m[kvfeed.kvaddr]
	if kvfeed.vbnos == nil {
		return ErrorVBmap
	}
	return nil
}
