// concurrency model:
//
//                                                    Network socket
//                                                  ------------------
//                                                              ^
//                                      *common.VbConnectionMap |
//                                      []*common.VbKeyVersions |
//                                                              |
//                          NewClient()                         |
//                           |     |                            |
//                           (spawn)----------*--------- runTransmitter()
//                           |                |
//                           |                *--------- runTransmitter()
//         SendVbmap() --*-> genServer        |
//                       |        |           *--------- runTransmitter()
//   SendKeyVersions() --*        |                        ^   ^   ^
//                       |        |                        |   |   |
//             Close() --*        *------------------------*---*---*
//                                  *common.VbKeyVersions
//                                  *common.VbConnectionMap
//
// client behavior:
//
// - client API to push mutation messages to the other end. This is an all or
//   nothing client for each downstream host.
// - If caller receives an error value while calling an exported method on
//   Client, it is adviced to stop the client, its connection pool, and
//   wait for a reconnect request.

package dataport

import (
	"fmt"
	"net"
	"time"

	"github.com/couchbase/indexing/secondary/common"
)

// Client is an active client for each remote host, and there can be
// multiple connections opened with remote host for the same endpoint.
type Client struct {
	// immutable fields
	raddr     string
	conns     map[int]net.Conn
	connChans map[int]chan interface{}
	// manage vbucket maps
	conn2Vbs map[int][]string // connection <-> vbucket map
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// miscellaneous
	logPrefix string
}

// NewClient returns a pool of connection. Multiple connections, based
// on parameter `n`, can be used to speed up mutation transport across network.
// A vbucket is always binded to a connection and ensure that mutations within
// a vbucket are serialized.
func NewClient(raddr string, n int, flags TransportFlag) (c *Client, err error) {
	var conn net.Conn

	if n == 0 {
		panic("fatal: cannot open dataport-client with zero connections")
	}

	c = &Client{
		raddr:     raddr,
		conns:     make(map[int]net.Conn),
		connChans: make(map[int]chan interface{}),
		conn2Vbs:  make(map[int][]string),
		reqch:     make(chan []interface{}, common.KeyVersionsChannelSize),
		finch:     make(chan bool),
		logPrefix: fmt.Sprintf("[DataportClient:%q]", raddr),
	}
	// open connections with remote
	size := common.KeyVersionsChannelSize
	for i := 0; i < n; i++ {
		if conn, err = net.Dial("tcp", raddr); err != nil {
			common.Errorf("%v %v Dialing to %q\n", c.logPrefix, raddr, err)
			c.doClose()
			return nil, err
		}
		c.conns[i] = conn
		c.connChans[i] = make(chan interface{}, size)
		c.conn2Vbs[i] = make([]string, 0, 4) // TODO: avoid magic numbers
	}
	// spawn routines per connection.
	quitch := make(chan []string, len(c.conns)*2)
	for i, conn := range c.conns {
		go c.runTransmitter(conn, flags, c.connChans[i], quitch)
	}
	go c.genServer(c.reqch, quitch)
	return c, nil
}

// find the connection that has least number of vbuckets mapped.
func (c *Client) addVbucket(uuid string) (chan interface{}, int) {
	idx, min := 0, len(c.conn2Vbs[0])
	for i, uuids := range c.conn2Vbs {
		for _, activeUuid := range uuids { // error handling
			if activeUuid == uuid {
				err := fmt.Errorf("%v duplicated %v", c.logPrefix, uuid)
				panic(err)
			}
		}
		if len(uuids) < min {
			idx, min = i, len(uuids)
		}
	}
	c.conn2Vbs[idx] = append(c.conn2Vbs[idx], uuid)
	return c.connChans[idx], idx
}

func (c *Client) delVbucket(uuid string) {
	nmap := make(map[int][]string)
	for i, uuids := range c.conn2Vbs {
		nmap[i] = common.RemoveString(uuid, uuids)
	}
	c.conn2Vbs = nmap
}

// gen-server commands
const (
	clientCmdSendVbmap byte = iota + 1
	clientCmdSendKeyVersions
	clientCmdGetcontext
	clientCmdClose
)

// SendVbmap vbmap for this connection to the other end, synchronous call.
func (c *Client) SendVbmap(vbmap *common.VbConnectionMap) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{clientCmdSendVbmap, vbmap, respch}
	resp, err := common.FailsafeOp(c.reqch, respch, cmd, c.finch)
	return common.OpError(err, resp, 0)
}

// SendKeyVersions for one or more vbuckets to the other end, asynchronous call.
func (c *Client) SendKeyVersions(vbs []*common.VbKeyVersions) error {
	if vbs == nil || len(vbs) == 0 {
		return ErrorClientEmptyKeys
	}
	var respch chan []interface{}
	cmd := []interface{}{clientCmdSendKeyVersions, vbs}
	_, err := common.FailsafeOp(c.reqch, respch, cmd, c.finch)
	return err
}

// Getcontext from dataport client, synchronous call.
func (c *Client) Getcontext() ([]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{clientCmdGetcontext, respch}
	resp, err := common.FailsafeOp(c.reqch, respch, cmd, c.finch)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Close client and all its active connection with downstream, asynchronous call.
func (c *Client) Close() error {
	var respch chan []interface{}
	cmd := []interface{}{clientCmdClose}
	_, err := common.FailsafeOp(c.reqch, respch, cmd, c.finch)
	return err
}

// gen-server
func (c *Client) genServer(reqch chan []interface{}, quitch chan []string) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			common.Errorf("%v has paniced: %v\n", c.logPrefix, r)
		}
		c.doClose()
	}()

	vbChans := make(map[string]chan interface{})

loop:
	for {
		select {
		case msg := <-reqch: // from upstream
			switch msg[0].(byte) {
			case clientCmdSendVbmap:
				vbmap := msg[1].(*common.VbConnectionMap)
				respch := msg[2].(chan []interface{})
				vbChans = c.sendVbmap(vbmap, vbChans)
				respch <- []interface{}{nil}

			case clientCmdSendKeyVersions:
				vbs := msg[1].([]*common.VbKeyVersions)
				quit := c.sendKeyVersions(vbs, vbChans, quitch)
				if quit != nil && quit[0] == "quit" {
					break loop
				}

			case clientCmdGetcontext:
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{vbChans, c.conn2Vbs}

			case clientCmdClose:
				break loop
			}

		case msg := <-quitch: // from downstream
			if msg[0] == "quit" {
				break loop
			}
		}
	}
}

// sendVbmap to the other end, carrying connection -> vbuckets map.
func (c *Client) sendVbmap(
	vbmap *common.VbConnectionMap,
	vbChans map[string]chan interface{}) map[string]chan interface{} {

	vbmaps := make(map[int]*common.VbConnectionMap)
	for i := range c.conn2Vbs {
		vbmaps[i] = &common.VbConnectionMap{
			Bucket:   vbmap.Bucket,
			Vbuckets: make([]uint16, 0, len(vbmap.Vbuckets)),
			Vbuuids:  make([]uint64, 0, len(vbmap.Vbuuids)),
		}
	}
	var idx int

	// connection channels.
	idxMap := make(map[int][]uint16)
	for i, vbno := range vbmap.Vbuckets {
		uuid := common.ID(vbmap.Bucket, vbno)
		vbChans[uuid], idx = c.addVbucket(uuid)
		vbmaps[idx].Vbuckets = append(vbmaps[idx].Vbuckets, vbno)
		vbmaps[idx].Vbuuids = append(vbmaps[idx].Vbuuids, vbmap.Vbuuids[i])
		if _, ok := idxMap[idx]; !ok {
			idxMap[idx] = make([]uint16, 0)
		}
	}
	for idx, vbnos := range idxMap {
		common.Tracef(
			"%v mapped vbucket {%v,%v} on conn%v\n",
			c.logPrefix, vbmap.Bucket, vbnos, idx)
	}

	// send the new vbmap to the other end, for each connection.
	for i, vbmap := range vbmaps {
		c.connChans[i] <- vbmap
	}
	return vbChans
}

// send mutations for a set of vbuckets, update vbucket channels based on
// StreamBegin and StreamEnd.
func (c *Client) sendKeyVersions(
	vbs []*common.VbKeyVersions,
	vbChans map[string]chan interface{},
	quitch chan []string) []string {

	var idx int

	for _, vb := range vbs {
		if len(vb.Kvs) == 0 {
			common.Warnf("%v empty mutations\n", c.logPrefix)
			continue
		}

		fin, l := false, len(vb.Kvs)

		if vb.Kvs[0].Commands[0] == common.StreamBegin { // first mutation
			vbChans[vb.Uuid], idx = c.addVbucket(vb.Uuid)
			common.Tracef(
				"%v mapped vbucket {%v,%v} on conn%v\n",
				c.logPrefix, vb.Bucket, vb.Vbucket, idx)
		}

		if vb.Kvs[l-1].Commands[0] == common.StreamEnd { // last mutation
			fin = true
		}

		select {
		case vbChans[vb.Uuid] <- vb:
			if fin {
				common.Infof("%v {%v,%v} ended\n", c.logPrefix, vb.Bucket, vb.Vbucket)
				c.delVbucket(vb.Uuid)
				delete(vbChans, vb.Uuid)
			}

		case msg := <-quitch:
			return msg
		}
	}
	return nil
}

// close all connections with downstream host.
func (c *Client) doClose() (err error) {
	recoverClose := func(payloadch chan interface{}, conn net.Conn) {
		defer func() {
			if r := recover(); r != nil {
				common.Errorf("%v panic closing %v\n", c.logPrefix, r)
				err = common.ErrorClosed
			}
		}()
		close(payloadch)
		conn.Close()
	}
	// close connections
	for i, payloadch := range c.connChans {
		recoverClose(payloadch, c.conns[i])
	}
	close(c.finch)
	common.Infof("%v closed", c.logPrefix)
	return
}

// per vbucket routine pushes *VbConnectionMap / *VbKeyVersions to other end.
func (c *Client) runTransmitter(
	conn net.Conn,
	flags TransportFlag,
	payloadch chan interface{},
	quitch chan []string) {

	laddr := conn.LocalAddr().String()
	defer func() {
		if r := recover(); r != nil {
			common.Errorf("%v fatal %v panic\n", c.logPrefix, laddr)
		}
		quitch <- []string{"quit", laddr}
	}()

	pkt := NewTransportPacket(common.MaxDataportPayload, flags)
	transmit := func(payload interface{}) bool {
		if err := pkt.Send(conn, payload); err != nil {
			common.Errorf("%v transport %q `%v`\n", c.logPrefix, laddr, err)
			return false
		}
		return true
	}

	timeout := time.Tick(common.TransmitBufferTimeout * time.Millisecond)
	vbs := make([]*common.VbKeyVersions, 0, 1000) // TODO: avoid magic numbers

	resetAcc := func() {
		for _, vb := range vbs {
			vb.Free()
		}
		vbs = vbs[:0] // reset buffer
	}

loop:
	for {
		select {
		case payload, ok := <-payloadch:
			if !ok {
				break loop
			}

			switch val := payload.(type) {
			case *common.VbConnectionMap:
				if transmit(val) == false {
					break loop
				}

			case *common.VbKeyVersions:
				vbs = append(vbs, val)
				if len(vbs) > 100 { // TODO: avoid magic number
					if transmit(vbs) == false {
						break loop
					}
					resetAcc()
				}
			}

		case <-timeout:
			if len(vbs) > 0 && transmit(vbs) == false {
				break loop
			}
			resetAcc()

		case <-c.finch:
			break loop
		}
	}
}
