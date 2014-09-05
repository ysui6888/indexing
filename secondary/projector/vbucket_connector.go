// Connector connecting VbucketRountine to downstream end points

package projector

import (
	"fmt"
	"errors"
	c "github.com/couchbase/indexing/secondary/common"
	pc "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
)

var ErrorInvalidDataForVbucketRoutineConnector = errors.New("secondary.invalidDataForVbucketRoutineConnector")
var ErrorInvalidDownStreamNodeForVbucketRoutineConnector = errors.New("secondary.invalidDownStreamNodeForVbucketRoutineConnector")

type VbucketRoutineConnector struct {
	endpoints map[string]*Endpoint // outgoing endpoints
	logPrefix string
}

// data type that VbucketRoutineConnector accepts
type VbucketRoutineConnectorData struct {
	bucket string  
	vbno   uint16 
	vbuuid uint64
	kvMap map[string]*c.KeyVersions  // KeyVersions map using endpoint.raddr as key
}


func NewVbucketRoutineConnector(endpoints map[string]*Endpoint, vrRepr string) (*VbucketRoutineConnector) {
	vrc := &VbucketRoutineConnector{
		endpoints : endpoints,
		logPrefix : fmt.Sprintf("[%v]", vrRepr),
	}
	return vrc
}

func NewVbucketRoutineConnectorData(bucket string, vbno uint16, vbuuid uint64, kvMap map[string]*c.KeyVersions) (*VbucketRoutineConnectorData) {
	vrd := &VbucketRoutineConnectorData{
		bucket : bucket,
		vbno : vbno,
		vbuuid : vbuuid,
		kvMap : kvMap,
	}
	return vrd
}

// implements Connector
func(vrc *VbucketRoutineConnector) Forward(data interface{}) error{
	// only VbucketRoutineConnectorData type data is accepted
	vrcData, ok := data.(*VbucketRoutineConnectorData)
	if !ok {
		return ErrorInvalidDataForVbucketRoutineConnector
	}

	for _, endpoint := range vrc.endpoints {
		kv := vrcData.kvMap[endpoint.raddr]
		if kv.Length() == 0 {
						continue
					}
		// sending to endpoint might fail, we don't care
		endpoint.Receive(NewEndpointData(vrcData.bucket, vrcData.vbno, vrcData.vbuuid, kv))
	}
	return nil
}

func(vrc *VbucketRoutineConnector) DownStreams () map[string]pc.Part {
	parts := make(map[string]pc.Part)
	for raddr, endpoint := range vrc.endpoints {
		parts[raddr] = endpoint
	}
	return parts
}

func(vrc *VbucketRoutineConnector) AddDownStream (partId string, part pc.Part) error{
	endpoint, ok := part.(*Endpoint)
	if !ok {
		return ErrorInvalidDownStreamNodeForVbucketRoutineConnector
	}

	vrc.endpoints[partId] = endpoint
	return nil
}

// convenience API
func(vrc *VbucketRoutineConnector) SetDownStreams (endpoints map[string]*Endpoint) {
	vrc.endpoints = endpoints
}
