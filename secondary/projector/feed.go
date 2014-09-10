// Feed is the central program around which adminport, bucket_feed, kvfeed,
// engines and endpoint algorithms are organized.

package projector

import (
	"errors"
	"fmt"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
)

// error codes

// ErrorInvalidBucket
var ErrorInvalidBucket = errors.New("feed.invalidBucket")

// ErrorRequestNotSubscriber
var ErrorRequestNotSubscriber = errors.New("feed.requestNotSubscriber")

// ErrorStartingEndpoint
var ErrorStartingEndpoint = errors.New("feed.startingEndpoint")

// ErrorInvalidStartingSettingForFeed
var ErrorInvalidStartingSettingsForFeed = errors.New("secondary.invalidStartingSettingsForFeed")

// Feed is mutation stream - for maintenance, initial-load, catchup etc...
type Feed struct {
	projector *Projector // immutable
	// embed GenericPipeline to support generic pipeline functionalities
	*pp.GenericPipeline
	engines   map[uint64]*Engine
	// timestamp feedback
	failoverTimestamps map[string]*protobuf.TsVbuuid // indexed by bucket name
	kvTimestamps       map[string]*protobuf.TsVbuuid // indexed by bucket name
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// misc.
	logPrefix string
	stats     c.Statistics
}

func (feed *Feed) getProjector() *Projector {
	return feed.projector
}

func (feed *Feed) repr() string {
	return fmt.Sprintf("%v:%v", feed.projector.repr(), feed.Topic())
}

func (feed *Feed) spawnBucketFeeds(pools, buckets []string) error {
	if len(pools) != len(buckets) {
		c.Errorf("%v pools and buckets mistmatch !", feed.logPrefix)
	}

	kvaddrs := feed.projector.getKVNodes()

	// fresh start of a mutation stream.
	for i, bucket := range buckets {
		// bucket-feed
		bfeed, err := NewBucketFeed(feed, kvaddrs, pools[i], bucket)
		if err != nil {
			return err
		}
		// initialse empty Timestamps objects for return values.
		feed.failoverTimestamps[bucket] = protobuf.NewTsVbuuid(bucket, c.MaxVbuckets)
		feed.kvTimestamps[bucket] = protobuf.NewTsVbuuid(bucket, c.MaxVbuckets)
		feed.Sources()[bucket] = bfeed
	}
	return nil
}

// gen-server API commands
const (
	fCmdRequestFeed byte = iota + 1
	fCmdUpdateFeed
	fCmdAddEngines
	fCmdUpdateEngines
	fCmdDeleteEngines
	fCmdRepairEndpoints
	fCmdGetStatistics
	fCmdCloseFeed
)

// RequestFeed to start a new mutation stream, synchronous call.
//
// if error is returned then upstream instances of BucketFeed and KVFeed are
// shutdown and application must retry the request or fall-back.
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if KVFeed is already closed.
func (feed *Feed) RequestFeed(request RequestReader) error {
	if request == nil {
		return ErrorArgument
	} else if _, ok := request.(Subscriber); ok == false {
		return ErrorRequestNotSubscriber
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRequestFeed, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// UpdateFeed will start / restart / shutdown upstream vbuckets and update
// downstream engines and endpoints, synchronous call.
//
// returns failover-timetamp and kv-timestamp (restart seqno. after honoring
// rollback)
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if Feed is already closed.
func (feed *Feed) UpdateFeed(request RequestReader) error {
	if request == nil {
		return ErrorArgument
	} else if _, ok := request.(Subscriber); ok == false {
		return ErrorRequestNotSubscriber
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdUpdateFeed, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// AddEngines will add new engines for this Feed, synchronous call.
//
// - error if Feed is already closed.
func (feed *Feed) AddEngines(request Subscriber) error {
	if request == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddEngines, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// UpdateEngines will update active engines for this Feed. This happens when
// routing algorithm is affected or topology changes for one or more entities,
// synchronous call.
//
// - error if Feed is already closed.
func (feed *Feed) UpdateEngines(request Subscriber) error {
	if request == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdUpdateEngines, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// DeleteEngines from active set of engines for this Feed. This happens when
// one or more entities are deleted, synchronous call.
//
// - error if Feed is already closed.
func (feed *Feed) DeleteEngines(request Subscriber) error {
	if request == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDeleteEngines, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// RepairEndpoints will restart downstream endpoints if it is already dead,
// synchronous call.
func (feed *Feed) RepairEndpoints(endpoints []string) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRepairEndpoints, endpoints, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// GetStatistics will recursively get statistics for feed and its underlying
// workers.
func (feed *Feed) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(map[string]interface{})
}

// CloseFeed will shutdown this feed and upstream and downstream instances,
// synchronous call.
func (feed *Feed) CloseFeed() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdCloseFeed, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

func (feed *Feed) genServer(reqch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			c.Errorf("%v ... paniced %v !\n", feed.logPrefix, r)
			feed.doClose()
		}
	}()

loop:
	for {
		msg := <-reqch
		switch msg[0].(byte) {
		case fCmdRequestFeed:
				req, respch := msg[1].(RequestReader), msg[2].(chan []interface{})
				respch <- []interface{}{feed.requestFeed(req)}
		case fCmdUpdateFeed:
			req, respch := msg[1].(RequestReader), msg[2].(chan []interface{})
			respch <- []interface{}{feed.updateFeed(req)}

		case fCmdAddEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.addEngines(req)}

		case fCmdUpdateEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.updateEngines(req)}

		case fCmdDeleteEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.deleteEngines(req)}

		case fCmdRepairEndpoints:
			endpoints := msg[1].([]string)
			respch := msg[2].(chan []interface{})
			respch <- []interface{}{feed.repairEndpoints(endpoints)}

		case fCmdGetStatistics:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{feed.getStatistics()}

		case fCmdCloseFeed:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{feed.doClose()}
			break loop
		}
	}
}

// start a new feed.
func (feed *Feed) requestFeed(req RequestReader) (err error) {
	var engines map[uint64]*Engine

	subscr := req.(Subscriber)
	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	if err = feed.buildEndpoints(routers, true/*bReplace*/); err != nil {
		return err
	}
	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
		return err
	}

	// order of bucket, restartTimestamp, failoverTimestamp and kvTimestamp
	// are preserved
	for bucket, emap := range bucketWiseEngines(engines) {
		bfeed := feed.Sources()[bucket].(*BucketFeed)
		if bfeed == nil {
			return ErrorInvalidBucket
		}
		err := bfeed.RequestFeed(req, feed.endpoints(), emap)
		if err != nil {
			return err
		}
		
	    // After refactoring, failover-timestamps and kv-timestamps are expected to be computed differently
		// 1. failover-timestamps should be retrieved in a separate RequestFailoverLog request
		// 2. In case that bfeed.RequestFeed() returns an error indicating that UprFeed.UprRequestStream() failed with 
		// a rollback sequence number, feed should extract the rollback sequence number from error and use it to 
		// re-compute and update the corresponding kv-timestamp
		
		// aggregate failover-timestamps, kv-timestamps for all buckets
		/*failTs = feed.failoverTimestamps[bucket].Union(failTs)
		feed.failoverTimestamps[bucket] = failTs
		kvTs = feed.kvTimestamps[bucket].Union(kvTs)
		feed.kvTimestamps[bucket] = kvTs*/
	}
	if len(engines) == 0 {
		c.Warnf("%v empty engines !\n", feed.logPrefix)
	} else {
		c.Infof("%v started ...\n", feed.logPrefix)
	}
	feed.engines = engines
	return nil
}

// start, restart, shutdown vbuckets in an active feed and/or update
// downstream engines and endpoints.
func (feed *Feed) updateFeed(req RequestReader) (err error) {
	var engines map[uint64]*Engine

	pools := req.GetPools()
	buckets := req.GetBuckets()

	subscr := req.(Subscriber)
	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	if evaluators != nil && routers != nil {
		if engines, err = feed.buildEngines(evaluators, routers); err != nil {
			return err
		}
	}

	// whether to delete buckets from feed.
	if req.IsDelBuckets() {
		for _, bucket := range buckets {
			feed.Sources()[bucket].Stop()
			delete(feed.Sources(), bucket)
			delete(feed.failoverTimestamps, bucket)
			delete(feed.kvTimestamps, bucket)
			for uuid, engine := range feed.engines {
				if engine.evaluator.Bucket() == bucket {
					delete(feed.engines, uuid)
				}
			}
			// endpoints are not deleted. if none of the active engines send
			// data to an endpoint, endpoint commits harakiri.
		}
		return nil
	}

	if routers != nil {
		err = feed.buildEndpoints(routers, true/*bReplace*/)
		if err != nil {
			return err
		}
	}

	// whether to add new buckets to feed.
	if req.IsAddBuckets() {
		if err := feed.spawnBucketFeeds(pools, buckets); err != nil {
			feed.doClose()
			return err
		}
		// spawnBucketFeeds no longer starts the BucketFeeds spawned. explicitly start them
		settings := feed.getProjector().constructStartSettings(req)
		for _, bucket := range buckets {
			feed.Sources()[bucket].Start(settings)
		}
		for uuid, engine := range engines {
			feed.engines[uuid] = engine
		}
	}

	// order of bucket, restartTimestamp, failoverTimestamp and kvTimestamp
	// are preserved
	for bucket, emap := range bucketWiseEngines(engines) {
		bfeed := feed.Sources()[bucket].(*BucketFeed)
		if bfeed == nil {
			return ErrorInvalidBucket
		}
		err := bfeed.UpdateFeed(req, feed.endpoints(), emap)
		if err != nil {
			return err
		}
		
		// After refactoring, failover-timestamps and kv-timestamps are expected to be computed differently
		// 1. failover-timestamps should be retrieved in a separate RequestFailoverLog request
		// 2. In case that bfeed.UpdateFeed() returns an error indicating that UprFeed.UprRequestStream() failed with 
		// a rollback sequence number, feed should extract the rollback sequence number from error and use it to 
		// re-compute and update the corresponding kv-timestamp
		
		// update failover-timestamps, kv-timestamps
		/*if req.IsShutdown() {
			failTs = feed.failoverTimestamps[bucket].FilterByVbuckets(
				c.Vbno32to16(failTs.Vbnos))
			kvTs = feed.kvTimestamps[bucket].FilterByVbuckets(
				c.Vbno32to16(kvTs.Vbnos))
		} else {
			failTs = feed.failoverTimestamps[bucket].Union(failTs)
			kvTs = feed.kvTimestamps[bucket].Union(kvTs)
		}
		feed.failoverTimestamps[bucket] = failTs
		feed.kvTimestamps[bucket] = kvTs*/
	}
	// TODO the following statement used to be there and disappeared. Is it intentional? 
	// feed.engines = engines
	c.Infof("%v update ... done\n", feed.logPrefix)
	return nil
}

// index topology has changed, update it.
func (feed *Feed) addEngines(subscr Subscriber) (err error) {
	var engines map[uint64]*Engine

	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	// union of existing endpoints and new endpoints, if any.
	if err = feed.buildEndpoints(routers, false/*bReplace*/); err != nil {
		return err
	}

	// union of existing engines and new engines.
	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
		return err
	}
	for uuid, engine := range feed.engines {
		engines[uuid] = engine
	}

	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.Sources()[bucket]; ok {
			if err = bfeed.(*BucketFeed).UpdateEngines(feed.endpoints(), emap); err != nil {
				return
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	feed.engines = engines
	c.Infof("%v add engines ... done\n", feed.logPrefix)
	return
}

// index topology has changed, update it.
// TODO: Remove this - if update can be replace with delete and add, then that
// should be the recommended way to update engines.
func (feed *Feed) updateEngines(subscr Subscriber) (err error) {
	var engines map[uint64]*Engine

	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	if err = feed.buildEndpoints(routers, true/*bReplace*/); err != nil {
		return err
	}
	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
		return err
	}

	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.Sources()[bucket]; ok {
			if err =  bfeed.(*BucketFeed).UpdateEngines(feed.endpoints(), emap); err != nil {
				return
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	feed.engines = engines
	c.Infof("%v update engines ... done\n", feed.logPrefix)
	return
}

// index is deleted, delete all of its downstream
func (feed *Feed) deleteEngines(subscr Subscriber) (err error) {
	var engines map[uint64]*Engine

	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	// we don't delete endpoints, since it might be shared with other engines.
	// endpoint routine will commit harakiri if no engines are sending them
	// data.

	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
		return err
	}

	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.Sources()[bucket]; ok {
			uuids := make([]uint64, 0, len(emap))
			for uuid := range emap {
				uuids = append(uuids, uuid)
			}
			if err = bfeed.(*BucketFeed).DeleteEngines(feed.endpoints(), uuids); err != nil {
				return
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	for uuid := range engines {
		c.Infof("%v engine %v deleted ...\n", feed.logPrefix, uuid)
		delete(feed.engines, uuid)
	}
	c.Infof("%v delete engines ... done\n", feed.logPrefix)
	return
}

// repair endpoints, restart engines and update bucket-feed
func (feed *Feed) repairEndpoints(raddrs []string) (err error) {
	// repair endpoints
	targets := feed.Targets()
	for raddr, target := range targets {
		endpoint := target.(*Endpoint)
		if raddrs != nil && len(raddrs) > 0 && !c.HasString(raddr, raddrs) {
			if endpoint.Ping() {
				targets[raddr] = endpoint
			}
			continue
		}
		c.Infof("%v restarting endpoint %q ...\n", feed.logPrefix, raddr)
		endpoint, err = feed.startEndpoint(raddr, endpoint.coord)
		if err != nil {
			return err
		}
		if endpoint != nil {
			targets[raddr] = endpoint
		}
	}

	// new set of engines
	engines := make(map[uint64]*Engine)
	for uuid, engine := range feed.engines {
		engine = NewEngine(feed, uuid, engine.evaluator, engine.router)
		engines[uuid] = engine
	}

	// update Engines with BucketFeed
	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.Sources()[bucket]; ok {
			if err = bfeed.(*BucketFeed).UpdateEngines(feed.endpoints(), emap); err != nil {
				return err
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	feed.engines = engines
	c.Infof("%v repair endpoints ... done\n", feed.logPrefix)
	return nil
}

func (feed *Feed) getStatistics() map[string]interface{} {
	bfeeds, _ := c.NewStatistics(feed.stats.Get("bfeeds"))
	endpoints, _ := c.NewStatistics(feed.stats.Get("endpoints"))
	feed.stats.Set("engines", feed.engineNames())
	for bucketn, bfeed := range feed.Sources() {
		bfeeds.Set(bucketn, bfeed.(*BucketFeed).GetStatistics())
	}
	for raddr, target := range feed.Targets() {
		endpoints.Set(raddr, target.(*Endpoint).GetStatistics())
	}
	feed.stats.Set("bfeeds", bfeeds)
	feed.stats.Set("endpoints", endpoints)
	return feed.stats.ToMap()
}

func (feed *Feed) doClose() (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v doClose() paniced: %v !\n", feed.Topic(), r)
		}
	}()

	for key, bfeed := range feed.Sources() { // shutdown upstream
		bfeed.Stop()
		delete(feed.Sources(), key)
	}
	for key, endpoint := range feed.Targets() { // shutdown downstream
		endpoint.Stop()
		delete(feed.Targets(), key)
	}
	// shutdown
	close(feed.finch)
	feed.engines = nil
	c.Infof("%v ... stopped\n", feed.logPrefix)
	return
}

// build per bucket uuid->engine map using Subscriber interface. `engines` and
// `endpoints` are updated inplace.
func (feed *Feed) buildEngines(
	evaluators map[uint64]c.Evaluator,
	routers map[uint64]c.Router) (map[uint64]*Engine, error) {

	// Rebuild new set of engines
	newengines := make(map[uint64]*Engine)
	for uuid, evaluator := range evaluators {
		engine := NewEngine(feed, uuid, evaluator, routers[uuid])
		newengines[uuid] = engine
	}
	return newengines, nil
}

// organize engines based on buckets, engine is associated with one bucket.
func bucketWiseEngines(engines map[uint64]*Engine) map[string]map[uint64]*Engine {
	bengines := make(map[string]map[uint64]*Engine)
	if engines == nil {
		return bengines
	}

	for uuid, engine := range engines {
		bucket := engine.evaluator.Bucket()
		emap, ok := bengines[bucket]
		if !ok {
			emap = make(map[uint64]*Engine)
			bengines[bucket] = emap
		}
		emap[uuid] = engine
	}
	return bengines
}

// start endpoints for listed topology, an endpoint is not started if it is
// already active (ping-ok).
// btw, we don't close endpoints, instead we let go of them.

// buildEndpoints builds endpoints from routers and updates the endpoints in feed with them
// if bReplace is true, the resulting endpoints are the new set of endpoints built from routers
// if bReplace is false, the resulting endpoints are a union of the endpoints built from routers and the existing endpoints in feed
func (feed *Feed) buildEndpoints(
	routers map[uint64]c.Router, bReplace bool) error {
	
	targets := feed.Targets()
	// routerEndpoints will hold the set of endpoints built from routers
	routerEndpoints := make(map[string]*Endpoint)

	for _, router := range routers {
		for _, raddr := range router.UuidEndpoints() {
			if target, ok := targets[raddr]; (!ok) || (!target.(*Endpoint).Ping()) {
				endpoint, err := feed.startEndpoint(raddr, false)
				if err != nil {
					return err
				} else if endpoint != nil {
					routerEndpoints[raddr] = endpoint
					// need to add new endpoint to targets so that subsequent routers will see it and not attempt to re-add it
					targets[raddr] = endpoint
				}
			} else {
			    // if end point already exists in targets, copy it over to routerEndpoints so that routerEndpoints contains the complete set of end points from routers
				routerEndpoints[raddr] = targets[raddr].(*Endpoint)
			}
		}
		// endpoint for coordinator
		coord := router.CoordinatorEndpoint()
		if coord != "" {
			if target, ok := targets[coord]; (!ok) || (!target.(*Endpoint).Ping()) {
				endpoint, err := feed.startEndpoint(coord, true)
				if err != nil {
					return err
				} else if endpoint != nil {
					routerEndpoints[coord] = endpoint
					targets[coord] = endpoint
				}
			} else {
				routerEndpoints[coord] = targets[coord].(*Endpoint)
			}
		}
	}
	
	if bReplace {
		// if replacing, use routerEndPoints to re-populate targets
		for raddr := range targets {
			delete(targets, raddr)
		}
		for raddr, endpoint := range routerEndpoints {
			targets[raddr] = endpoint
		}	
	} else {
	    // if not replacing, targets already contain both old entries and new entries from routers. no op
	}

	
	return nil
}

func (feed *Feed) startEndpoint(raddr string, coord bool) (endpoint *Endpoint, err error) {
	endpoint, err = NewEndpoint(feed, raddr, c.ConnsPerEndpoint, coord)
	if err != nil {
		return nil, err
	}
	err = endpoint.Start(nil)
	if err != nil {
		return nil, err
	}
	// TODO: send vbmap to the new endpoint.
	// for _, kvTs := range feed.kvTimestamps {
	//     vbmap := feed.vbTs2Vbmap(kvTs)
	//     if err = endpoint.SendVbmap(vbmap); err != nil {
	//         return nil, err
	//     }
	// }
	return endpoint, nil
}

func (feed *Feed) validateSubscriber(subscriber Subscriber) (map[uint64]c.Evaluator, map[uint64]c.Router, error) {
	evaluators, err := subscriber.GetEvaluators()
	if err != nil {
		return nil, nil, err
	}
	routers, err := subscriber.GetRouters()
	if err != nil {
		return nil, nil, err
	}
	if len(evaluators) != len(routers) {
		err = ErrorInconsistentFeed
		c.Errorf("%v error %v, len() mismatch", feed.logPrefix, err)
		return nil, nil, err
	}
	for uuid := range evaluators {
		if _, ok := routers[uuid]; ok == false {
			err = ErrorInconsistentFeed
			c.Errorf("%v error %v, uuid mismatch", feed.logPrefix, err)
			return nil, nil, err
		}
	}
	return evaluators, routers, nil
}

func (feed *Feed) engineNames() []string {
	names := make([]string, 0, len(feed.engines))
	for uuid := range feed.engines {
		names = append(names, fmt.Sprintf("%v", uuid))
	}
	return names
}

func (feed *Feed) endpointNames() []string {
	raddrs := make([]string, 0, len(feed.Targets()))
	for raddr := range feed.Targets() {
		raddrs = append(raddrs, raddr)
	}
	return raddrs
}

// return endpoints in feed as a map of Endpoints
func (feed *Feed) endpoints() map[string]*Endpoint{
	endpoints := make(map[string]*Endpoint)
	
   	for key, target := range feed.Targets() {
   		endpoints[key] = target.(*Endpoint)
   	}
   	return endpoints
}

// implements Pipeline
func (feed *Feed) Start(settings map[string]interface{}) error {
	projector, request, err := parseStartSettings(settings);
	if  err != nil {
		return err
	}
	
	// Feed specific startup work
	
	// initializes feed. initialization parameters cannot be passed in at feed construction time and it is done here instead
	feed.projector = projector
	feed.logPrefix = fmt.Sprintf("[%v]", feed.repr())
	
	pools := request.GetPools()
	buckets := request.GetBuckets()
			
	// create bucketfeeds
	if err := feed.spawnBucketFeeds(pools, buckets); err != nil {
		feed.Stop()
		return err
	}

	go feed.genServer(feed.reqch)
	c.Infof("%v activated ...\n", feed.logPrefix)
	
	// call Start() method on generic pipeline. This will get BucketFeeds and Endpoints started 
	if err := feed.GenericPipeline.Start(settings); err != nil {
		return err
	}
	
	// preserve the old scheme prior to refactoring to minimize code changes.
	// this will eventually get KVFeeds started
	return feed.RequestFeed(request)

}

// override Stop() API in feed.GenericPipeline
func (feed *Feed) Stop() error {
	return feed.CloseFeed()
}

// utility function
func parseStartSettings(settings map[string]interface{}) (*Projector, *protobuf.MutationStreamRequest, error) {
	// validate starting settings for Feed and extract required info.
	// it should contain a projector object and a MutationStreamRequest
	var projector *Projector
	var request *protobuf.MutationStreamRequest 
	
	if len(settings) != 1 {
		return nil, nil, ErrorInvalidStartingSettingsForFeed
	}
	for _, setting := range settings {
		if settingArr, ok := setting.([2]interface{}); !ok {
			return nil, nil, ErrorInvalidStartingSettingsForFeed
		} else {
			if len(settingArr) != 2 {
			 	return nil, nil, ErrorInvalidStartingSettingsForFeed
			 } else {
			 	if projector, ok = settingArr[0].(*Projector); !ok {
			 		return nil, nil, ErrorInvalidStartingSettingsForFeed
			 	} else {
					if request, ok = settingArr[1].(*protobuf.MutationStreamRequest); !ok {
						return nil, nil, ErrorInvalidStartingSettingsForFeed
					}
				}
			}
		}
	}
	return projector, request, nil
}
