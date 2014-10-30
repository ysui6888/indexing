// Connector connecting KVFeed to downstream nodes, i.e., KVFeed

package projector

import (
	"errors"
	"fmt"
	"strconv"
	"github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
	pc "github.com/Xiaomei-Zhang/goxdcr/common"
	component "github.com/Xiaomei-Zhang/goxdcr/component"
)

var ErrorInvalidDataForKVFeedConnector = errors.New("secondary.invalidDataForKVFeedConnector")
var ErrorInvalidDownStreamNodeForKVFeedConnector = errors.New("secondary.invalidDownStreamNodeForKVFeedConnector")

type KVFeedConnector struct {
	component.AbstractComponent
	vbuckets map[uint16]*VbucketRoutine // map of VbucketRoutine using vbno as key
	bucketn string   // bucket name to be passed downstream to VbucketRoutines
	endpoints map[string]*Endpoint   // endpoints to be passed downstream to VbucketRoutines
	engines map[uint64]*Engine   // engines to be passed downstream to VbucketRoutines
	
	kvfeedRepr string   // repr of parent KVFeed
	logPrefix string
}

func NewKVFeedConnector(bucketn, kvfeedRepr string) (*KVFeedConnector, error) {
	kvc := &KVFeedConnector{
		AbstractComponent: component.NewAbstractComponent("KVFeedConnector"),
	    vbuckets:  make(map[uint16]*VbucketRoutine),
		bucketn : bucketn,
		kvfeedRepr: kvfeedRepr,
		endpoints: make(map[string]*Endpoint),
		engines: make(map[uint64]*Engine),
	}
	kvc.logPrefix = fmt.Sprintf("[%v]", kvc.repr())
	return kvc, nil
}

func (kvc *KVFeedConnector) repr() string {
	// use the same repr as KVFeed. should we append "Connector" to the end to distinguish?
	return fmt.Sprintf("%v", kvc.kvfeedRepr)
}

// scatterMutation to vbuckets.
func (kvc *KVFeedConnector) scatterMutation(
	m *mc.UprEvent) {

	vbno := m.VBucket

	switch m.Opcode {
	case gomemcached.UPR_STREAMREQ:
		if _, ok := kvc.vbuckets[vbno]; ok {
			fmtstr := "%v, duplicate OpStreamRequest for %v\n"
			c.Errorf(fmtstr, kvc.logPrefix, m.VBucket)
		} else {
			var err error
			m.VBuuid, m.Seqno, err = m.FailoverLog.Latest()
			if err != nil {
				c.Errorf("%v vbucket(%v) %v", kvc.logPrefix, m.VBucket, err)

			} else {
				vr := NewVbucketRoutine(kvc.kvfeedRepr, kvc.bucketn, vbno, m.VBuuid)
				connector := NewVbucketRoutineConnector(kvc.endpoints, vr.repr())
				vr.SetConnector(connector)
				kvc.vbuckets[vbno] = vr
				// vr is no longer started by NewVbucketRoutine() and needs to be explicitly started
				vr.Start(nil)
				vr.UpdateEngines(kvc.endpoints, kvc.engines)
				vr.Receive(m)
				c.Tracef("%v, StreamRequest for %v\n", kvc.logPrefix, vbno)
			}
		}

	case gomemcached.UPR_STREAMEND:
		if vr, ok := kvc.vbuckets[vbno]; !ok {
			fmtstr := "%v, duplicate OpStreamEnd for %v\n"
			c.Errorf(fmtstr, kvc.logPrefix, m.VBucket)
		} else {
			vr.Stop()
			delete(kvc.vbuckets, vbno)
			c.Tracef("%v, StreamRequest for %v\n", kvc.logPrefix, vbno)
		}

	case gomemcached.UPR_MUTATION, gomemcached.UPR_DELETION, gomemcached.UPR_SNAPSHOT:
		if vr, ok := kvc.vbuckets[vbno]; ok {
			if vr.vbuuid != m.VBuuid {
				fmtstr := "%v, vbuuid mismatch (%v:%v) for vbucket %v\n"
				c.Errorf(fmtstr, kvc.logPrefix, vr.vbuuid, m.VBuuid, m.VBucket)
				vr.Stop()
				delete(kvc.vbuckets, vbno)

			} else {
				vr.Receive(m)
			}
		}
	}
	return
}

// UpdateEngines update active set of engines and endpoints, synchronous call.
func (kvc *KVFeedConnector) UpdateEngines(endpoints map[string]*Endpoint, engines map[uint64]*Engine) error {
	// update endpoints and engines in connector itself, which will impact new VbucketRoutines to be created from the connector
	kvc.endpoints = endpoints
	kvc.engines = engines
	// update endpoints and engines in downstream VbucketRoutines
	for _, vr := range kvc.vbuckets {
		vr.UpdateEngines(endpoints, engines);
	}
	return nil
}

// DeleteEngines update active set of engines and endpoints, synchronous call.
func (kvc *KVFeedConnector) DeleteEngines(endpoints map[string]*Endpoint, engines []uint64) error {
	// update endpoints and delete engines in connector itself, which will impact new VbucketRoutines to be created from the connector
	kvc.endpoints = endpoints
	for _, engineKey := range engines {
		delete(kvc.engines, engineKey)
		c.Tracef("%v, deleted engine %v\n", kvc.logPrefix, engineKey)
	}
	// call DeleteEngines on downstream VbucketRoutines
	for _, vr := range kvc.vbuckets {
		vr.DeleteEngines(endpoints, engines)
	}
	
	return nil
}

// implements Connector
func(kvc *KVFeedConnector) Forward(data interface{}) error{
	// only *mc.UprEvent type data is accepted
	uprEvent, ok := data.(*mc.UprEvent)
	if !ok {
		return ErrorInvalidDataForKVFeedConnector
	}
	
	kvc.scatterMutation(uprEvent)
	
	return nil
}

func(kvc *KVFeedConnector) DownStreams () map[string]pc.Part {
	parts := make(map[string]pc.Part)
	for vbno, vr := range kvc.vbuckets {
		//convert vbno to string 
		parts[convertUintToString(vbno)] = vr
	}
	return parts
}

func(kvc *KVFeedConnector) AddDownStream (partId string, part pc.Part) error{
	vr, ok := part.(*VbucketRoutine)
	if !ok {
		return ErrorInvalidDownStreamNodeForKVFeedConnector
	}
	// convert string partId into a uint16 vbno
	vbno, err := convertStringToUint(partId)
	if err != nil {
		return err
	}
	kvc.vbuckets[uint16(vbno)] = vr
	return nil	
}

// utility functions for uint to string conversions for vbno, which is uint16 and needs to be converted to string to be used as Part Id
func convertUintToString (vbno uint16) string {
	return strconv.FormatUint(uint64(vbno), 10)
}

func convertStringToUint (vbnoStr string) (uint16, error) {
	if vbno, err := strconv.ParseUint(vbnoStr, 10/*base*/, 16/*bitSize*/); err == nil {
		return uint16(vbno), nil
	}
	return 0, ErrorInvalidDownStreamNodeForKVFeedConnector
}
