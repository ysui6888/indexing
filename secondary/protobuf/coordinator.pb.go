// Code generated by protoc-gen-go.
// source: coordinator.proto
// DO NOT EDIT!

package protobuf

import proto "code.google.com/p/goprotobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

// Posted by cluster manager to indicate a new coordinator. Error message
// will be sent as reponse.
type NewCoordinatorRequest struct {
	ConnectionAddr   *string `protobuf:"bytes,1,req,name=connectionAddr" json:"connectionAddr,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *NewCoordinatorRequest) Reset()         { *m = NewCoordinatorRequest{} }
func (m *NewCoordinatorRequest) String() string { return proto.CompactTextString(m) }
func (*NewCoordinatorRequest) ProtoMessage()    {}

func (m *NewCoordinatorRequest) GetConnectionAddr() string {
	if m != nil && m.ConnectionAddr != nil {
		return *m.ConnectionAddr
	}
	return ""
}

// Requested by Coordinator to Replica to fetch replica's StateContext version.
type StateContextCasRequest struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *StateContextCasRequest) Reset()         { *m = StateContextCasRequest{} }
func (m *StateContextCasRequest) String() string { return proto.CompactTextString(m) }
func (*StateContextCasRequest) ProtoMessage()    {}

type StateContextCasResponse struct {
	Version          *StateContextVersion `protobuf:"bytes,1,req,name=version" json:"version,omitempty"`
	XXX_unrecognized []byte               `json:"-"`
}

func (m *StateContextCasResponse) Reset()         { *m = StateContextCasResponse{} }
func (m *StateContextCasResponse) String() string { return proto.CompactTextString(m) }
func (*StateContextCasResponse) ProtoMessage()    {}

func (m *StateContextCasResponse) GetVersion() *StateContextVersion {
	if m != nil {
		return m.Version
	}
	return nil
}

// Requested by Coordinator for Replica's local StateContext.
type StateContextRequest struct {
	Version          *StateContextVersion `protobuf:"bytes,1,req,name=version" json:"version,omitempty"`
	XXX_unrecognized []byte               `json:"-"`
}

func (m *StateContextRequest) Reset()         { *m = StateContextRequest{} }
func (m *StateContextRequest) String() string { return proto.CompactTextString(m) }
func (*StateContextRequest) ProtoMessage()    {}

func (m *StateContextRequest) GetVersion() *StateContextVersion {
	if m != nil {
		return m.Version
	}
	return nil
}

type StateContextReponse struct {
	Version          *StateContextVersion `protobuf:"bytes,1,req,name=version" json:"version,omitempty"`
	Data             []byte               `protobuf:"bytes,2,req,name=data" json:"data,omitempty"`
	XXX_unrecognized []byte               `json:"-"`
}

func (m *StateContextReponse) Reset()         { *m = StateContextReponse{} }
func (m *StateContextReponse) String() string { return proto.CompactTextString(m) }
func (*StateContextReponse) ProtoMessage()    {}

func (m *StateContextReponse) GetVersion() *StateContextVersion {
	if m != nil {
		return m.Version
	}
	return nil
}

func (m *StateContextReponse) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

// Posted by Coordinator to Replica to update its local StateContext, response
// would be StateContextCasResponse.
type UpdateStateContext struct {
	Version          *StateContextVersion `protobuf:"bytes,1,req,name=version" json:"version,omitempty"`
	Data             []byte               `protobuf:"bytes,2,req,name=data" json:"data,omitempty"`
	XXX_unrecognized []byte               `json:"-"`
}

func (m *UpdateStateContext) Reset()         { *m = UpdateStateContext{} }
func (m *UpdateStateContext) String() string { return proto.CompactTextString(m) }
func (*UpdateStateContext) ProtoMessage()    {}

func (m *UpdateStateContext) GetVersion() *StateContextVersion {
	if m != nil {
		return m.Version
	}
	return nil
}

func (m *UpdateStateContext) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

// Requested by cluster manager whenever an actor joins or leaves the cluster.
type ActorClusterRequest struct {
	Actors           []*Actor `protobuf:"bytes,1,rep,name=actors" json:"actors,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *ActorClusterRequest) Reset()         { *m = ActorClusterRequest{} }
func (m *ActorClusterRequest) String() string { return proto.CompactTextString(m) }
func (*ActorClusterRequest) ProtoMessage()    {}

func (m *ActorClusterRequest) GetActors() []*Actor {
	if m != nil {
		return m.Actors
	}
	return nil
}

type ActorClusterResponse struct {
	Actors           []*Actor `protobuf:"bytes,1,rep,name=actors" json:"actors,omitempty"`
	Err              *Error   `protobuf:"bytes,2,opt,name=err" json:"err,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *ActorClusterResponse) Reset()         { *m = ActorClusterResponse{} }
func (m *ActorClusterResponse) String() string { return proto.CompactTextString(m) }
func (*ActorClusterResponse) ProtoMessage()    {}

func (m *ActorClusterResponse) GetActors() []*Actor {
	if m != nil {
		return m.Actors
	}
	return nil
}

func (m *ActorClusterResponse) GetErr() *Error {
	if m != nil {
		return m.Err
	}
	return nil
}

// Requested by an indexer joining the cluster, for which coordinator responds
// with list of indexes that are to be hosted by that indexer.
type IndexerInitRequest struct {
	Indexerid        *uint32 `protobuf:"varint,1,req,name=indexerid" json:"indexerid,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *IndexerInitRequest) Reset()         { *m = IndexerInitRequest{} }
func (m *IndexerInitRequest) String() string { return proto.CompactTextString(m) }
func (*IndexerInitRequest) ProtoMessage()    {}

func (m *IndexerInitRequest) GetIndexerid() uint32 {
	if m != nil && m.Indexerid != nil {
		return *m.Indexerid
	}
	return 0
}

type IndexerInitResponse struct {
	Instances        []*IndexInst `protobuf:"bytes,1,rep,name=instances" json:"instances,omitempty"`
	XXX_unrecognized []byte       `json:"-"`
}

func (m *IndexerInitResponse) Reset()         { *m = IndexerInitResponse{} }
func (m *IndexerInitResponse) String() string { return proto.CompactTextString(m) }
func (*IndexerInitResponse) ProtoMessage()    {}

func (m *IndexerInitResponse) GetInstances() []*IndexInst {
	if m != nil {
		return m.Instances
	}
	return nil
}

// Requested by an indexer once it is ready to receive mutation stream. Error
// message will be returned as response.
type IndexerReadyRequest struct {
	IndexUuids       []uint64 `protobuf:"varint,1,rep,name=indexUuids" json:"indexUuids,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *IndexerReadyRequest) Reset()         { *m = IndexerReadyRequest{} }
func (m *IndexerReadyRequest) String() string { return proto.CompactTextString(m) }
func (*IndexerReadyRequest) ProtoMessage()    {}

func (m *IndexerReadyRequest) GetIndexUuids() []uint64 {
	if m != nil {
		return m.IndexUuids
	}
	return nil
}

// Posted by indexer periodically to Coordinator, updating its high-watermark
// timestamp. Error message will be sent as response.
type NewHWTimestampRequest struct {
	TsVbFull         []string `protobuf:"bytes,1,rep" json:"TsVbFull,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *NewHWTimestampRequest) Reset()         { *m = NewHWTimestampRequest{} }
func (m *NewHWTimestampRequest) String() string { return proto.CompactTextString(m) }
func (*NewHWTimestampRequest) ProtoMessage()    {}

func (m *NewHWTimestampRequest) GetTsVbFull() []string {
	if m != nil {
		return m.TsVbFull
	}
	return nil
}

// Requested by Indexer. When an index is in IndexLoading state, this request
// will mean that it is done with initial-loading stream for the specified
// indexes. Error message will be sent as response.
type SwitchingToActiveRequest struct {
	IndexUuids       []uint64 `protobuf:"varint,1,rep,name=indexUuids" json:"indexUuids,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *SwitchingToActiveRequest) Reset()         { *m = SwitchingToActiveRequest{} }
func (m *SwitchingToActiveRequest) String() string { return proto.CompactTextString(m) }
func (*SwitchingToActiveRequest) ProtoMessage()    {}

func (m *SwitchingToActiveRequest) GetIndexUuids() []uint64 {
	if m != nil {
		return m.IndexUuids
	}
	return nil
}

// Requested by Indexer. When an index is in IndexActive state, this
// request will mean that it is done with rebalance catchup for specified
// index. Error message will be sent as response.
type RebalanceToActiveRequest struct {
	IndexUuids       []uint64 `protobuf:"varint,1,rep,name=indexUuids" json:"indexUuids,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *RebalanceToActiveRequest) Reset()         { *m = RebalanceToActiveRequest{} }
func (m *RebalanceToActiveRequest) String() string { return proto.CompactTextString(m) }
func (*RebalanceToActiveRequest) ProtoMessage()    {}

func (m *RebalanceToActiveRequest) GetIndexUuids() []uint64 {
	if m != nil {
		return m.IndexUuids
	}
	return nil
}

// Requested by admin console to start index rebalance. Error message will be
// sent as response.
type IndexRebalanceRequest struct {
	IndexUuid        *uint64        `protobuf:"varint,1,req,name=indexUuid" json:"indexUuid,omitempty"`
	Tp               *TestPartition `protobuf:"bytes,2,opt,name=tp" json:"tp,omitempty"`
	XXX_unrecognized []byte         `json:"-"`
}

func (m *IndexRebalanceRequest) Reset()         { *m = IndexRebalanceRequest{} }
func (m *IndexRebalanceRequest) String() string { return proto.CompactTextString(m) }
func (*IndexRebalanceRequest) ProtoMessage()    {}

func (m *IndexRebalanceRequest) GetIndexUuid() uint64 {
	if m != nil && m.IndexUuid != nil {
		return *m.IndexUuid
	}
	return 0
}

func (m *IndexRebalanceRequest) GetTp() *TestPartition {
	if m != nil {
		return m.Tp
	}
	return nil
}

func init() {
}
