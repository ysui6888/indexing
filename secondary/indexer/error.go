// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

type errCode int16

const (
	ERROR_PANIC errCode = iota

	//Slab Manager
	ERROR_SLAB_INIT
	ERROR_SLAB_BAD_ALLOC_REQUEST
	ERROR_SLAB_INTERNAL_ALLOC_ERROR
	ERROR_SLAB_MEM_LIMIT_EXCEED
	ERROR_SLAB_INTERNAL_ERROR

	//Stream Reader
	ERROR_STREAM_INIT
	ERROR_STREAM_READER_UNKNOWN_COMMAND
	ERROR_STREAM_READER_STREAM_SHUTDOWN
	ERROR_STREAM_READER_RESTART_VBUCKETS
	ERROR_STREAM_READER_UNKNOWN_ERROR
	ERROR_STREAM_READER_PANIC

	//Mutation Manager
	ERROR_MUT_MGR_INTERNAL_ERROR
	ERROR_MUT_MGR_STREAM_ALREADY_OPEN
	ERROR_MUT_MGR_STREAM_ALREADY_CLOSED
	ERROR_MUT_MGR_UNKNOWN_COMMAND
	ERROR_MUT_MGR_UNCLEAN_SHUTDOWN
	ERROR_MUT_MGR_PANIC

	//Mutation Queue
	ERROR_MUTATION_QUEUE_INIT

	//Timekeeper
	ERROR_TK_UNKNOWN_STREAM

	//KVSender
	ERROR_KVSENDER_UNKNOWN_INDEX
	ERROR_KVSENDER_STREAM_ALREADY_OPEN
	ERROR_KVSENDER_STREAM_REQUEST_ERROR
	ERROR_KV_SENDER_UNKNOWN_STREAM
	ERROR_KVSENDER_STREAM_ALREADY_CLOSED

	//ScanCoordinator
	ERROR_SCAN_COORD_UNKNOWN_COMMAND
	ERROR_SCAN_COORD_INTERNAL_ERROR

	//INDEXER
	ERROR_INDEX_ALREADY_EXISTS
	ERROR_INDEXER_INTERNAL_ERROR
	ERROR_INDEX_BUILD_IN_PROGRESS
	ERROR_INDEXER_UNKNOWN_INDEX
)

type errSeverity int16

const (
	FATAL errSeverity = iota
	NORMAL
)

type errCategory int16

const (
	MESSAGING errCategory = iota
	STORAGE
	MUTATION_QUEUE
	TOPOLOGY
	STREAM_READER
	SLAB_MANAGER
	MUTATION_MANAGER
	TIMEKEEPER
	SCAN_COORD
	INDEXER
)

type Error struct {
	code     errCode
	severity errSeverity
	category errCategory
	cause    error
	msg      string
}
