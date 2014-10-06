// projector's adminport.

package projector

import (
	"errors"

	"code.google.com/p/goprotobuf/proto"
	ap "github.com/ysui6888/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
)

// error codes

// ErrorFeedAlreadyActive
var ErrorFeedAlreadyActive = errors.New("projector.adminport.feedAlreadyActive")

// ErrorInvalidTopic
var ErrorInvalidTopic = errors.New("projector.adminport.invalidTopic")

// list of requests handled by this adminport
var reqVbmap = &protobuf.VbmapRequest{}
var reqFailoverLog = &protobuf.FailoverLogRequest{}
var reqMutationFeed = &protobuf.MutationStreamRequest{}
var reqUpdateFeed = &protobuf.UpdateMutationStreamRequest{}
var reqSubscribeFeed = &protobuf.SubscribeStreamRequest{}
var reqRepairEndpoints = &protobuf.RepairDownstreamEndpoints{}
var reqShutdownFeed = &protobuf.ShutdownStreamRequest{}
var reqStats = c.Statistics{}

// admin-port entry point
func mainAdminPort(laddr string, p *Projector) {
	var err error

	reqch := make(chan ap.Request)
	server := ap.NewHTTPServer("projector", laddr, c.AdminportURLPrefix, reqch, new(ap.Handler))
	server.Register(reqVbmap)
	server.Register(reqFailoverLog)
	server.Register(reqMutationFeed)
	server.Register(reqUpdateFeed)
	server.Register(reqSubscribeFeed)
	server.Register(reqRepairEndpoints)
	server.Register(reqShutdownFeed)
	server.Register(reqStats)

	server.Start()

loop:
	for {
		select {
		case req, ok := <-reqch: // admin requests are serialized here
			if ok == false {
				break loop
			}
			msg := req.GetMessage()
			if response, err := p.handleRequest(msg, server); err == nil {
				req.Send(response)
			} else {
				req.SendError(err)
			}
		}
	}
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
	}
	c.Infof("%v exited !\n", p.logPrefix)
	server.Stop()
}

func (p *Projector) handleRequest(
	msg ap.MessageMarshaller,
	server ap.Server) (response ap.MessageMarshaller, err error) {

	switch request := msg.(type) {
	case *protobuf.VbmapRequest:
		response = p.doVbmapRequest(request)
	case *protobuf.FailoverLogRequest:
		response = p.doFailoverLog(request)
	case *protobuf.MutationStreamRequest:
		response = p.doMutationFeed(request)
	case *protobuf.UpdateMutationStreamRequest:
		response = p.doUpdateFeed(request)
	case *protobuf.SubscribeStreamRequest:
		response = p.doSubscribeFeed(request)
	case *protobuf.RepairDownstreamEndpoints:
		response = p.doRepairEndpoints(request)
	case *protobuf.ShutdownStreamRequest:
		response = p.doShutdownFeed(request)
	case c.Statistics:
		response = p.doStatistics(request, server)
	default:
		err = c.ErrorInvalidRequest
	}
	return response, err
}

// handler neither use upstream connections nor disturbs upstream data path.
func (p *Projector) doVbmapRequest(request *protobuf.VbmapRequest) ap.MessageMarshaller {
	c.Debugf("%v doVbmapRequest\n", p.logPrefix)
	response := &protobuf.VbmapResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	kvaddrs := request.GetKvaddrs()

	bucket, err := p.getBucket(pooln, bucketn)
	if err != nil {
		c.Errorf("%v %s, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	response.Kvaddrs = make([]string, 0, len(kvaddrs))
	response.Kvvbnos = make([]*protobuf.Vbuckets, 0, len(kvaddrs))
	bucket.Refresh()
	m, err := bucket.GetVBmap(kvaddrs)
	if err != nil {
		c.Errorf("%v %s, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	for kvaddr, vbnos := range m {
		response.Kvaddrs = append(response.Kvaddrs, kvaddr)
		response.Kvvbnos = append(
			response.Kvvbnos, &protobuf.Vbuckets{Vbnos: c.Vbno16to32(vbnos)})
	}
	return response
}

// handler neither use upstream connections nor disturbs upstream data path.
func (p *Projector) doFailoverLog(request *protobuf.FailoverLogRequest) ap.MessageMarshaller {
	c.Debugf("%v doFailoverLog\n", p.logPrefix)
	response := &protobuf.FailoverLogResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	vbuckets := request.GetVbnos()

	bucket, err := p.getBucket(pooln, bucketn)
	if err != nil {
		c.Errorf("%v %s, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	protoFlogs := make([]*protobuf.FailoverLog, 0, len(vbuckets))
	vbnos := c.Vbno32to16(vbuckets)
	if flogs, err := bucket.GetFailoverLogs(vbnos); err == nil {
		for vbno, flog := range flogs {
			vbuuids := make([]uint64, 0, len(flog))
			seqnos := make([]uint64, 0, len(flog))
			for _, x := range flog {
				vbuuids = append(vbuuids, x[0])
				seqnos = append(seqnos, x[1])
			}
			protoFlog := &protobuf.FailoverLog{
				Vbno:    proto.Uint32(uint32(vbno)),
				Vbuuids: vbuuids,
				Seqnos:  seqnos,
			}
			protoFlogs = append(protoFlogs, protoFlog)
		}
	} else {
		c.Errorf("%v %s.GetFailoverLogs() %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	response.Logs = protoFlogs
	return response
}

// start a new mutation feed, on error the feed is shutdown.
func (p *Projector) doMutationFeed(request *protobuf.MutationStreamRequest) ap.MessageMarshaller {
	var err error

	c.Debugf("%v doMutationFeed()\n", p.logPrefix)
	response := protobuf.NewMutationStreamResponse(request)

	topic := request.GetTopic()
	bucketns := request.GetBuckets()

	feed, err := p.GetFeed(topic)
	if err == nil { // only fresh feed to be started
		c.Errorf("%v %v\n", p.logPrefix, ErrorTopicExist)
		response.UpdateErr(ErrorFeedAlreadyActive)
		return response
	}

	if feed, err = p.NewFeed(topic, request); err == nil {
		// we expect failoverTimestamps and kvTimestamps to be populated.
		failTss := make([]*protobuf.TsVbuuid, 0, len(bucketns))
		kvTss := make([]*protobuf.TsVbuuid, 0, len(bucketns))
		for _, bucketn := range bucketns {
			failTss = append(failTss, feed.failoverTimestamps[bucketn])
			kvTss = append(kvTss, feed.kvTimestamps[bucketn])
		}
		response.UpdateTimestamps(failTss, kvTss)
	} else {
		if feed != nil {
			feed.Stop() // on error stop the feed
		}
		response.UpdateErr(err)
	}
	return response
}

// update an already existing feed.
// - start / restart / shutdown one or more vbucket streams
// - update partition tables that will affect routing.
// - update topology that will add or remove endpoints from existing list.
//
// upon error, it is left to the caller to shutdown the feed.
func (p *Projector) doUpdateFeed(request *protobuf.UpdateMutationStreamRequest) ap.MessageMarshaller {
	var err error

	c.Debugf("%v doUpdateFeed()\n", p.logPrefix)
	response := protobuf.NewMutationStreamResponse(request)

	topic := request.GetTopic()
	bucketns := request.GetBuckets()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		response.UpdateErr(err)
		return response
	}

	if err = feed.UpdateFeed(request); err == nil {
		// gather latest set of timestamps for each bucket, provided request
		// is not for deleting the bucket.
		if !request.IsDelBuckets() {
			// we expect failoverTimestamps and kvTimestamps to be re-populated.
			failTss := make([]*protobuf.TsVbuuid, 0, len(bucketns))
			kvTss := make([]*protobuf.TsVbuuid, 0, len(bucketns))
			for _, bucketn := range bucketns {
				failTss = append(failTss, feed.failoverTimestamps[bucketn])
				kvTss = append(kvTss, feed.kvTimestamps[bucketn])
			}
			response.UpdateTimestamps(failTss, kvTss)
		}
	} else {
		response.UpdateErr(err)
	}
	return response
}

// add or remove endpoints.
func (p *Projector) doSubscribeFeed(request *protobuf.SubscribeStreamRequest) ap.MessageMarshaller {
	c.Debugf("%v doSubscribeFeed()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	if request.IsAddEngines() {
		err = feed.AddEngines(request)
	} else if request.IsUpdateEngines() {
		err = feed.UpdateEngines(request)
	} else if request.IsDeleteEngines() {
		err = feed.DeleteEngines(request)
	} else {
		err = c.ErrorInvalidRequest
		c.Errorf("%v %v\n", p.logPrefix, err)
	}
	return protobuf.NewError(err)
}

// restart connection with specified list of endpoints.
func (p *Projector) doRepairEndpoints(request *protobuf.RepairDownstreamEndpoints) ap.MessageMarshaller {
	c.Debugf("%v doRepairEndpoints()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic)
	if err == nil { // only existing feed
		err = feed.RepairEndpoints(request.GetEndpoints())
	} else {
		c.Errorf("%v %v\n", p.logPrefix, err)
	}
	return protobuf.NewError(err)
}

// shutdown feed, all upstream vbuckets and downstream endpoints.
func (p *Projector) doShutdownFeed(request *protobuf.ShutdownStreamRequest) ap.MessageMarshaller {
	c.Debugf("%v doShutdownFeed()\n", p.logPrefix)
	topic := request.GetTopic()

	_, err := p.GetFeed(topic)
	if err == nil { // only existing feed
	    // this will stop the feed
		p.DelFeed(topic)
	} else {
		c.Errorf("%v %v\n", p.logPrefix, err)
	}
	return protobuf.NewError(err)
}

// get projector statistics.
func (p *Projector) doStatistics(request c.Statistics, adminport ap.Server) ap.MessageMarshaller {
	c.Debugf("%v doStatistics()\n", p.logPrefix)
	stats := p.GetStatistics()
	stats.Set("adminport", adminport.GetStatistics())
	return stats
}
