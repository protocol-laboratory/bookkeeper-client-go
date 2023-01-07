// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bk

import (
	"github.com/go-zookeeper/zk"
	"github.com/protocol-laboratory/bookkeeper-codec-go/codec"
	"github.com/protocol-laboratory/bookkeeper-codec-go/pb"
	"strings"
	"time"
)

type ClientConfig struct {
	zkServers []string
}

type Client struct {
	zkConnection *zk.Conn
	zkEvents     <-chan zk.Event
	Layout       Layout
}

func NewClient(config ClientConfig) (c *Client, err error) {
	c = &Client{}
	c.zkConnection, c.zkEvents, err = zk.Connect(config.zkServers, time.Duration(30)*time.Second)
	if err != nil {
		return nil, err
	}
	layout, err := c.getLedgerLayout()
	if err != nil {
		return nil, err
	}
	if layout == LayoutHierarchicalString {
		c.Layout = LayoutHierarchical
	} else {
		return nil, ErrUnsupportedLayout
	}
	return c, nil
}

func (c *Client) DeleteLedger(ledgerId int64) error {
	ledgerPath := c.getLedgerPath(ledgerId)
	return c.zkConnection.Delete(ledgerPath, -1)
}

func (c *Client) ReadLedgerMetadata(ledgerId int64) (metadata *pb.LedgerMetadataFormat, err error) {
	ledgerPath := c.getLedgerPath(ledgerId)
	data, _, err := c.zkConnection.Get(ledgerPath)
	if err != nil {
		return nil, err
	}
	return codec.DecodeLedgerMetadata(data)
}

type LedgerIterateCallback interface {
	Process(ledgerId int64)
	Error(err error)
}

func (c *Client) IterateLedgers(callback LedgerIterateCallback) {
	switch c.Layout {
	case LayoutHierarchical:
		c.iterateLedgersHierarchical(callback)
	}
}

// iterateLedgersHierarchical /ledgers/00/0000/L0000
func (c *Client) iterateLedgersHierarchical(callback LedgerIterateCallback) {
	firstDirList, _, err := c.zkConnection.Children("/ledgers")
	if err != nil {
		callback.Error(err)
		return
	}
	for _, firstDir := range firstDirList {
		var secondDirList []string
		secondDirList, _, err = c.zkConnection.Children("/ledgers/" + firstDir)
		if err != nil {
			callback.Error(err)
			return
		}
		for _, secondDir := range secondDirList {
			var ledgerList []string
			ledgerList, _, err = c.zkConnection.Children("/ledgers/" + firstDir + "/" + secondDir)
			if err != nil {
				callback.Error(err)
				return
			}
			for _, name := range ledgerList {
				var ledgerId int64
				ledgerId, err = ledgerIdFromPathHierarchical(firstDir, secondDir, name)
				if err != nil {
					callback.Error(err)
					continue
				}
				callback.Process(ledgerId)
			}
		}
	}
}

func (c *Client) getLedgerPath(id int64) string {
	switch c.Layout {
	case LayoutHierarchical:
		return getLedgerPathHierarchical(id)
	}
	return ""
}

func (c *Client) getLedgerLayout() (string, error) {
	bytes, _, err := c.zkConnection.Get("/ledgers/LAYOUT")
	if err != nil {
		return "", err
	}
	factoryLine := strings.Split(string(bytes), "\n")[1]
	return strings.Split(factoryLine, ":")[0], nil
}

func (c *Client) Close() {
	c.zkConnection.Close()
}
