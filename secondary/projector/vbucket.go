// concurrency model:
//
//                           NewVbucketRoutine()
//                                   |
//                                   |               *---> endpoint
//                                (spawn)            |
//                                   |               *---> endpoint
//             Event() --*           |               |
//                       |--------> run -------------*---> endpoint
//     UpdateEngines() --*
//                       |
//     DeleteEngines() --*
//                       |
//             Close() --*

package projector

import (
	"fmt"
	"github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
	"time"
	"errors"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
)

var ErrorInvalidDataForVbucketRoutine = errors.New("secondary.invalidDataForVbucketRoutine")
var ErrorInvalidConnectorForVbucketRoutine = errors.New("secondary.invalidConnectorForVbucketRoutine")

// VbucketRoutine is immutable structure defined for each vbucket.
type VbucketRoutine struct {
	pp.AbstractPart  // Part
	bucket string  // immutable
	vbno   uint16  // immutable
	vbuuid uint64  // immutable
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// misc.
	kvfeedRepr string
	logPrefix string
	stats c.Statistics
}

// NewVbucketRoutine creates a new routine to handle this vbucket stream.
func NewVbucketRoutine(kvfeedRepr, bucket string, vbno uint16, vbuuid uint64) *VbucketRoutine {
	vr := &VbucketRoutine{
		kvfeedRepr: kvfeedRepr,
		bucket: bucket,
		vbno:   vbno,
		vbuuid: vbuuid,
		reqch:  make(chan []interface{}, c.MutationChannelSize),
		finch:  make(chan bool),
	}
	vr.logPrefix = fmt.Sprintf("[%v]", vr.repr())
	vr.stats = vr.newStats()
	
	// uses vbno as the part Id  for VbucketRoutine
	var isStarted_callback_func pp.IsStarted_Callback_Func = vr.IsStarted
	vr.AbstractPart = pp.NewAbstractPart(convertUintToString(vbno), &isStarted_callback_func)
	
	c.Infof("%v ... created\n", vr.logPrefix)
	return vr
}

func (vr *VbucketRoutine) repr() string {
	return fmt.Sprintf("vb %v:%v", vr.kvfeedRepr, vr.vbno)
}

const (
	vrCmdEvent byte = iota + 1
	vrCmdUpdateEngines
	vrCmdDeleteEngines
	vrCmdGetStatistics
	vrCmdClose
)

// Event will post an UprEvent, asychronous call.
func (vr *VbucketRoutine) Event(m *mc.UprEvent) error {
	if m == nil {
		return ErrorArgument
	}
	var respch chan []interface{}
	cmd := []interface{}{vrCmdEvent, m}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// UpdateEngines update active set of engines and endpoints, synchronous call.
func (vr *VbucketRoutine) UpdateEngines(endpoints map[string]*Endpoint, engines map[uint64]*Engine) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdUpdateEngines, endpoints, engines, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// DeleteEngines delete engines and update endpoints, synchronous call.
func (vr *VbucketRoutine) DeleteEngines(endpoints map[string]*Endpoint, engines []uint64) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdDeleteEngines, endpoints, engines, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// GetStatistics for this vbucket.
func (vr *VbucketRoutine) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return resp[0].(map[string]interface{})
}

// Close this vbucket routine and free its resources, synchronous call.
func (vr *VbucketRoutine) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdClose, respch}
	resp, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch) // synchronous call
	return c.OpError(err, resp, 0)
}

// routine handles data path for a single vbucket, never panics.
// TODO: statistics on data path must be fast.
func (vr *VbucketRoutine) run(reqch chan []interface{}) {
	var seqno uint64
	var heartBeat <-chan time.Time
	
	var engines map[uint64]*Engine

	stats := vr.stats
	uEngineCount := stats.Get("uEngines").(float64)
	dEngineCount := stats.Get("dEngines").(float64)
	beginCount := stats.Get("begins").(float64)
	sshotCount := stats.Get("snapshots").(float64)
	mutationCount := stats.Get("mutations").(float64)
	syncCount := stats.Get("syncs").(float64)

loop:
	for {
	inner:
		select {
		case msg := <-reqch:
			cmd := msg[0].(byte)
			switch cmd {
			case vrCmdUpdateEngines:
				if msg[1] != nil {
					// update down stream node map in connector
					vr.Connector().(*VbucketRoutineConnector).SetDownStreams(msg[1].(map[string]*Endpoint))
				}
				if msg[2] != nil {
					engines = msg[2].(map[uint64]*Engine)
				}
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}
				uEngineCount++
				vr.debugCtrlPath(
					msg[1].(map[string]*Endpoint),
					msg[2].(map[uint64]*Engine),
				)

			case vrCmdDeleteEngines:
				// update down stream node map in connector
				vr.Connector().(*VbucketRoutineConnector).SetDownStreams(msg[1].(map[string]*Endpoint))
				for _, uuid := range msg[2].([]uint64) {
					delete(engines, uuid)
				}
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}
				dEngineCount++
				vr.debugCtrlPath(msg[1].(map[string]*Endpoint), engines)

			case vrCmdGetStatistics:
				respch := msg[1].(chan []interface{})
				stats.Set("uEngines", uEngineCount)
				stats.Set("dEngines", dEngineCount)
				stats.Set("begins", beginCount)
				stats.Set("snapshots", sshotCount)
				stats.Set("mutations", mutationCount)
				stats.Set("syncs", syncCount)
				respch <- []interface{}{stats.ToMap()}

			case vrCmdEvent:
				m := msg[1].(*mc.UprEvent)
				// broadcast StreamBegin
				switch m.Opcode {
				case gomemcached.UPR_STREAMREQ:
					vr.sendToEndpoints(func(raddr string) *c.KeyVersions {
						kv := c.NewKeyVersions(0, m.Key, 1)
						kv.AddStreamBegin()
						return kv
					})
					tickTs := c.VbucketSyncTimeout * time.Millisecond
					heartBeat = time.Tick(tickTs)
					beginCount++
					break inner // breaks out of select{}

				case gomemcached.UPR_SNAPSHOT:
					c.Debugf("%v received snapshot %v %v (type %v)\n",
						vr.logPrefix, m.SnapstartSeq, m.SnapendSeq, m.SnapshotType)
					vr.sendToEndpoints(func(raddr string) *c.KeyVersions {
						kv := c.NewKeyVersions(0, m.Key, 1)
						kv.AddSnapshot(m.SnapshotType, m.SnapstartSeq, m.SnapendSeq)
						return kv
					})
					sshotCount++
					break inner // breaks out of select
				}

				// UprMutation, UprDeletion, UprExpiration

				seqno = m.Seqno

				// prepare a KeyVersions for each endpoint.
				kvForEndpoints := make(map[string]*c.KeyVersions)
				for raddr := range vr.Connector().DownStreams() {
					kv := c.NewKeyVersions(seqno, m.Key, len(engines))
					kvForEndpoints[raddr] = kv
				}
				// for each engine populate endpoint KeyVersions.
				for _, engine := range engines {
					engine.AddToEndpoints(m, kvForEndpoints)
				}
				// send kv to corresponding endpoint
				vr.sendKVMapToEndpoints(kvForEndpoints)
				mutationCount++

			case vrCmdClose:
				respch := msg[1].(chan []interface{})
				vr.doClose(seqno)
				respch <- []interface{}{nil}
				break loop
			}

		case <-heartBeat:
			if len(vr.Connector().DownStreams()) > 0 {
				vr.sendToEndpoints(func(raddr string) *c.KeyVersions {
					c.Tracef("%v sync %v to %q", vr.logPrefix, syncCount, raddr)
					kv := c.NewKeyVersions(seqno, nil, 1)
					kv.AddSync()
					return kv
				})
				syncCount++
			}
		}
	}
}

// close this vbucket routine
func (vr *VbucketRoutine) doClose(seqno uint64) {
	vr.sendToEndpoints(func(raddr string) *c.KeyVersions {
		kv := c.NewKeyVersions(seqno, nil, 1)
		kv.AddStreamEnd()
		return kv
	})
	close(vr.finch)
	c.Infof("%v ... stopped\n", vr.logPrefix)
}

func (vr *VbucketRoutine) sendToEndpoints(fn func(string) *c.KeyVersions) error {
    kvForEndPoints := make(map[string]*c.KeyVersions)
	for raddr := range vr.Connector().DownStreams() {
		kv := fn(raddr)
		kvForEndPoints[raddr] = kv
	}
	return vr.sendKVMapToEndpoints(kvForEndPoints)
}

// send KeyVersions map to endpoints. Each endpoint may get different KeyVersions
func (vr *VbucketRoutine) sendKVMapToEndpoints(kvForEndPoints map[string]*c.KeyVersions) error {
	return vr.Connector().Forward(NewVbucketRoutineConnectorData(vr.bucket, vr.vbno, vr.vbuuid, kvForEndPoints))
}

func (vr *VbucketRoutine) debugCtrlPath(endpoints map[string]*Endpoint, engines map[uint64]*Engine) {
	if endpoints != nil {
		for _, endpoint := range endpoints {
			c.Debugf("%v, knows endpoint %v\n", vr.logPrefix, endpoint.timestamp)
		}
	}
	if engines != nil {
		for uuid := range engines {
			c.Debugf("%v, knows engine %v\n", vr.logPrefix, uuid)
		}
	}
}

// implements Part
func (vr *VbucketRoutine) Start(settings map[string]interface{}) error {
	go vr.run(vr.reqch)
	c.Infof("%v ... started\n", vr.logPrefix)
	return nil
}
func (vr *VbucketRoutine) Stop() error {
	err := vr.Close()
	return err
}

// this is the only method used and implemented
func (vr *VbucketRoutine) Receive (data interface{}) error {
	if m, ok := data.(*mc.UprEvent); !ok {
		return ErrorInvalidDataForVbucketRoutine
	} else {
		return vr.Event(m) 
	}
}

func (vr *VbucketRoutine) IsStarted() bool {
	return false
}
