//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
	"sync"
)

//Flusher is the only component which does read/dequeue from a MutationQueue.
//As MutationQueue has a restriction of only single reader and writer per vbucket,
//flusher should not be invoked concurrently for a single MutationQueue.
type Flusher interface {

	//PersistUptoTS will flush the mutation queue upto Timestamp provided.
	//Can be stopped anytime by closing StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till TS.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	PersistUptoTS(q MutationQueue, streamId common.StreamId, indexInstMap common.IndexInstMap,
		indexPartnMap IndexPartnMap, ts Timestamp, stopch StopChannel) MsgChannel

	//DrainUptoTS will flush the mutation queue upto Timestamp
	//provided without actually persisting it.
	//Can be stopped anytime by closing the StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	DrainUptoTS(q MutationQueue, streamId common.StreamId, ts Timestamp,
		stopch StopChannel) MsgChannel

	//Persist will keep flushing the mutation queue till caller closes
	//the stop channel.Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	Persist(q MutationQueue, streamId common.StreamId, indexInstMap common.IndexInstMap,
		indexPartnMap IndexPartnMap, stopch StopChannel) MsgChannel

	//Drain will keep flushing the mutation queue till caller closes
	//the stop channel without actually persisting the mutations.
	//Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	Drain(q MutationQueue, streamId common.StreamId, stopch StopChannel) MsgChannel

	//IsTimestampGreaterThanQueueLWT checks if each Vbucket in the Queue
	//has mutation with Seqno lower than the corresponding Seqno present
	//in the specified timestamp.
	IsQueueLWTLowerThanTimestamp(q MutationQueue, ts Timestamp) bool

	//GetQueueLWT returns the lowest seqno for each vbucket in the queue
	GetQueueLWT(q MutationQueue) Timestamp

	//GetQueueHWT returns the highest seqno for each vbucket in the queue
	GetQueueHWT(q MutationQueue) Timestamp
}

type flusher struct {
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap
}

//NewFlusher returns new instance of flusher
func NewFlusher() *flusher {
	return &flusher{}
}

//PersistUptoTS will flush the mutation queue upto the
//Timestamp provided.  This function will be used when:
//1. Flushing Maintenance Queue
//2. Flushing Maintenance Catchup Queue
//3. Flushing Backfill Queue
//
//Can be stopped anytime by closing StopChannel.
//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) PersistUptoTS(q MutationQueue, streamId common.StreamId,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap,
	ts Timestamp, stopch StopChannel) MsgChannel {

	common.Infof("Flusher::PersistUptoTS StreamId: %v Timestamp: %v",
		streamId, ts)

	f.indexInstMap = common.CopyIndexInstMap(indexInstMap)
	f.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, ts, true, stopch, msgch)
	return msgch
}

//DrainUptoTS will flush the mutation queue upto the Timestamp
//provided without actually persisting it.
//Can be stopped anytime by closing the StopChannel.
//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) DrainUptoTS(q MutationQueue, streamId common.StreamId,
	ts Timestamp, stopch StopChannel) MsgChannel {

	common.Infof("Flusher::DrainUptoTS StreamId: %v Timestamp: %v",
		streamId, ts)

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, ts, false, stopch, msgch)
	return msgch
}

//Persist will keep flushing the mutation queue till caller closes
//the stop channel.  This function will be used when:
//1. Flushing Backfill Catchup Queue
//
//Can be stopped anytime by closing the StopChannel.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) Persist(q MutationQueue, streamId common.StreamId,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap,
	stopch StopChannel) MsgChannel {

	common.Infof("Flusher::Persist StreamId: %v", streamId)

	f.indexInstMap = common.CopyIndexInstMap(indexInstMap)
	f.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, nil, true, stopch, msgch)
	return msgch
}

//Drain will keep flushing the mutation queue till caller closes
//the stop channel without actually persisting the mutations
//Can be stopped anytime by closing the StopChannel.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) Drain(q MutationQueue, streamId common.StreamId,
	stopch StopChannel) MsgChannel {

	common.Infof("Flusher::Drain StreamId: %v", streamId)

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, nil, false, stopch, msgch)
	return msgch
}

//flushQueue starts and waits for actual workers to flush the mutation queue.
//This function will close the done channel once all workers have finished.
//It also listens on the stop channel and will stop all workers if stop signal is received.
func (f *flusher) flushQueue(q MutationQueue, streamId common.StreamId,
	ts Timestamp, persist bool, stopch StopChannel, msgch MsgChannel) {

	var wg sync.WaitGroup
	var i uint16

	numVbuckets := q.GetNumVbuckets()

	//create stop channel for each worker, to propagate the stop signal
	workerStopChannels := make([]StopChannel, numVbuckets)

	//create msg channel for workers to provide messages
	workerMsgCh := make(MsgChannel)

	for i = 0; i < numVbuckets; i++ {
		wg.Add(1)
		if ts == nil {
			go f.flushSingleVbucket(q, streamId, Vbucket(i),
				persist, workerStopChannels[i], workerMsgCh, &wg)
		} else {
			go f.flushSingleVbucketUptoSeqno(q, streamId, Vbucket(i),
				ts[i], persist, workerStopChannels[i], workerMsgCh, &wg)
		}
	}

	allWorkersDoneCh := make(DoneChannel)

	//wait for all workers to finish
	go func() {
		common.Tracef("Flusher::flushQueue Waiting for workers to finish Stream %v", streamId)
		wg.Wait()
		//send signal on channel to indicate all workers have finished
		common.Tracef("Flusher::flushQueue All workers finished for Stream %v", streamId)
		close(allWorkersDoneCh)
	}()

	//wait for upstream to signal stop or for all workers to signal done
	//or workers to send any message
	select {
	case <-stopch:
		common.Debugf("Flusher::flushQueue Stopping All Workers")
		//stop all workers
		for _, ch := range workerStopChannels {
			close(ch)
		}
		//wait for all workers to stop
		<-allWorkersDoneCh
		common.Debugf("Flusher::flushQueue Stopped All Workers")

		//wait for notification of all workers finishing
	case <-allWorkersDoneCh:

		//handle any message from workers
	case m, ok := <-workerMsgCh:
		if ok {
			//TODO identify the messages and handle
			//For now, just relay back the message
			msgch <- m
		}
		return
	}

	msgch <- &MsgSuccess{}
}

//flushSingleVbucket is the actual implementation which flushes the given queue
//for a single vbucket till stop signal
func (f *flusher) flushSingleVbucket(q MutationQueue, streamId common.StreamId,
	vbucket Vbucket, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg *sync.WaitGroup) {

	defer wg.Done()

	common.Tracef("Flusher::flushSingleVbucket Started worker to flush vbucket: "+
		"%v for stream: %v", vbucket, streamId)

	mutch, qstopch, err := q.Dequeue(vbucket)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *MutationKeys

	//Process till supervisor asks to stop on the channel
	for ok {
		select {
		case mut, ok = <-mutch:
			if ok {
				if !persist {
					//No persistence is required. Just skip this mutation.
					continue
				}
				f.flushSingleMutation(mut, streamId)
			}
		case <-stopch:
			qstopch <- true
			return
		}
	}
}

//flushSingleVbucket is the actual implementation which flushes the given queue
//for a single vbucket till the given seqno or till the stop signal(whichever is earlier)
func (f *flusher) flushSingleVbucketUptoSeqno(q MutationQueue, streamId common.StreamId,
	vbucket Vbucket, seqno Seqno, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg *sync.WaitGroup) {

	defer wg.Done()

	common.Tracef("Flusher::flushSingleVbucketUptoSeqno Started worker to flush vbucket: "+
		"%v till Seqno: %v for Stream: %v", vbucket, seqno, streamId)

	mutch, err := q.DequeueUptoSeqno(vbucket, seqno)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *MutationKeys

	//Read till the channel is closed by queue indicating it has sent all the
	//sequence numbers requested
	for ok {
		select {
		case mut, ok = <-mutch:
			if ok {
				if !persist {
					//No persistence is required. Just skip this mutation.
					continue
				}
				f.flushSingleMutation(mut, streamId)
			}
		}
	}
}

//flushSingleMutation talks to persistence layer to store the mutations
//Any error from persistence layer is sent back on workerMsgCh
func (f *flusher) flushSingleMutation(mut *MutationKeys, streamId common.StreamId) {

	switch streamId {

	case common.MAINT_STREAM, common.INIT_STREAM:
		f.flush(mut, streamId)

	default:
		common.Errorf("Flusher::flushSingleMutation Invalid StreamId: %v", streamId)
	}
}

func (f *flusher) flush(mut *MutationKeys, streamId common.StreamId) {

	common.Tracef("Flusher::flush Flushing Maintenance Stream Mutations %v", mut)

	var processedUpserts []common.IndexInstId
	for i, cmd := range mut.commands {

		var idxInst common.IndexInst
		var ok bool
		if idxInst, ok = f.indexInstMap[mut.uuids[i]]; !ok {
			common.Errorf("Flusher::flush Unknown Index Instance Id %v. "+
				"Skipped Mutation Key %v", mut.uuids[i], mut.keys[i])
			continue
		}

		//Skip this mutation if the index doesn't belong to the stream being flushed
		if streamId != idxInst.Stream {
			common.Tracef("Flusher::flush \n\tFound Mutation For IndexId: %v Stream: %v In "+
				"Stream: %v. Skipped Mutation Key %v", idxInst.InstId, idxInst.Stream,
				streamId, mut.keys[i])
			continue
		}

		switch cmd {

		case common.Upsert:
			processedUpserts = append(processedUpserts, mut.uuids[i])

			f.processUpsert(mut, i)

		case common.Deletion:
			f.processDelete(mut, i)

		case common.UpsertDeletion:

			var skipUpsertDeletion bool
			//if Upsert has been processed for this IndexInstId,
			//skip processing UpsertDeletion
			for _, id := range processedUpserts {
				if id == mut.uuids[i] {
					skipUpsertDeletion = true
				}
			}

			if skipUpsertDeletion {
				continue
			} else {
				f.processDelete(mut, i)
			}

		default:
			common.Errorf("Flusher::flush Unknown mutation type received. Skipped %v",
				mut.keys[i])
		}
	}
}

func (f *flusher) processUpsert(mut *MutationKeys, i int) {

	var key Key
	var value Value
	var err error

	if key, err = NewKey(mut.keys[i], mut.docid); err != nil {

		common.Errorf("Flusher::processUpsert Error Generating Key"+
			"From Mutation: %v. Skipped. Error: %v", mut.keys[i], err)
		return
	}

	if value, err = NewValue(mut.keys[i], mut.docid, mut.meta.vbucket,
		mut.meta.seqno); err != nil {

		common.Errorf("Flusher::processUpsert Error Generating Value"+
			"From Mutation: %v. Skipped. Error: %v", mut.keys[i], err)
		return
	}

	idxInst, _ := f.indexInstMap[mut.uuids[i]]

	partnId := idxInst.Pc.GetPartitionIdByPartitionKey(mut.partnkeys[i])

	var partnInstMap PartitionInstMap
	var ok bool
	if partnInstMap, ok = f.indexPartnMap[mut.uuids[i]]; !ok {
		common.Errorf("Flusher::processUpsert Missing Partition Instance Map"+
			"for IndexInstId: %v. Skipped Mutation Key: %v", mut.uuids[i], mut.keys[i])
		return
	}

	if partnInst := partnInstMap[partnId]; ok {
		slice := partnInst.Sc.GetSliceByIndexKey(common.IndexKey(mut.keys[i]))
		if err := slice.Insert(key, value); err != nil {
			common.Errorf("Flusher::processUpsert Error Inserting Key: %v "+
				"Value: %v in Slice: %v. Error: %v", key, value, slice.Id(), err)
		}
	} else {
		common.Errorf("Flusher::processUpsert Partition Instance not found "+
			"for Id: %v Skipped Mutation Key: %v", partnId, mut.keys[i])
	}

}

func (f *flusher) processDelete(mut *MutationKeys, i int) {

	idxInst, _ := f.indexInstMap[mut.uuids[i]]

	partnId := idxInst.Pc.GetPartitionIdByPartitionKey(mut.partnkeys[i])

	var partnInstMap PartitionInstMap
	var ok bool
	if partnInstMap, ok = f.indexPartnMap[mut.uuids[i]]; !ok {
		common.Errorf("Flusher:processDelete Missing Partition Instance Map"+
			"for IndexInstId: %v. Skipped Mutation Key: %v", mut.uuids[i], mut.keys[i])
		return
	}

	if partnInst := partnInstMap[partnId]; ok {
		slice := partnInst.Sc.GetSliceByIndexKey(common.IndexKey(mut.keys[i]))
		if err := slice.Delete(mut.docid); err != nil {
			common.Errorf("Flusher::processDelete Error Deleting DocId: %v "+
				"from Slice: %v", mut.docid, slice.Id())
		}
	} else {
		common.Errorf("Flusher::processDelete Partition Instance not found "+
			"for Id: %v. Skipped Mutation Key: %v", partnId, mut.keys[i])
	}
}

//IsTimestampGreaterThanQueueLWT checks if each Vbucket in the Queue has
//mutation with Seqno lower than the corresponding Seqno present in the
//specified timestamp.
func (f *flusher) IsQueueLWTLowerThanTimestamp(q MutationQueue, ts Timestamp) bool {

	//each individual vbucket seqno should be lower than or equal to timestamp seqno
	for i, t := range ts {
		mut := q.PeekHead(Vbucket(i))
		if mut.meta.seqno > t {
			return false
		}
	}
	return true

}

//GetQueueLWT returns the lowest seqno for each vbucket in the queue
func (f *flusher) GetQueueLWT(q MutationQueue) Timestamp {

	var ts Timestamp
	var i uint16
	for i = 0; i < q.GetNumVbuckets(); i++ {
		if mut := q.PeekHead(Vbucket(i)); mut != nil {
			ts[i] = mut.meta.seqno
		} else {
			ts[i] = 0
		}
	}
	return ts
}

//GetQueueHWT returns the highest seqno for each vbucket in the queue
func (f *flusher) GetQueueHWT(q MutationQueue) Timestamp {

	var ts Timestamp
	var i uint16
	for i = 0; i < q.GetNumVbuckets(); i++ {
		if mut := q.PeekTail(Vbucket(i)); mut != nil {
			ts[i] = mut.meta.seqno
		} else {
			ts[i] = 0
		}
	}
	return ts
}
