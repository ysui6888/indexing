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
)

type StreamAddressMap map[common.StreamId]common.Endpoint

type StreamStatusMap map[common.StreamId]bool

// a generic channel which can be closed when you
// want someone to stop doing something
type StopChannel chan bool

// a generic channel which can be closed when you
// want to indicate the caller that you are done
type DoneChannel chan bool

type MsgChannel chan Message

type MutationChannel chan *MutationKeys

//IndexMutationQueue comprising of a mutation queue
//and a slab manager
type IndexerMutationQueue struct {
	queue   MutationQueue
	slabMgr SlabManager //slab allocator for mutation memory allocation
}

//IndexQueueMap is a map between IndexId and IndexerMutationQueue
type IndexQueueMap map[common.IndexInstId]IndexerMutationQueue

type Vbucket uint32
type Vbuuid uint64
type Seqno uint64

//MutationMeta represents meta information for a KV Mutation
type MutationMeta struct {
	bucket  string  //bucket for the mutation
	vbucket Vbucket //vbucket
	vbuuid  Vbuuid  //uuid for vbucket
	seqno   Seqno   // vbucket sequence number for this mutation
}

//MutationKeys holds the Secondary Keys from a single KV Mutation
type MutationKeys struct {
	meta      *MutationMeta
	docid     []byte               // primary document id
	uuids     []common.IndexInstId // list of unique ids, like index-ids
	commands  []byte               // list of commands for each index
	keys      [][][]byte           // list of key-versions for each index
	oldkeys   [][][]byte           // previous key-versions, if available
	partnkeys [][]byte             // list of partition keys
}

type ScanParams struct {
	scanType ScanType
	low      Key
	high     Key
	partnKey []byte
	incl     Inclusion
	limit    int64
}
