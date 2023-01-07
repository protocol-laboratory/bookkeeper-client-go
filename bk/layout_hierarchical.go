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
	"strconv"
	"strings"
)

func ledgerIdFromPathHierarchical(firstDir string, secondDir string, name string) (int64, error) {
	if !strings.HasPrefix(name, "L") {
		return 0, ErrInvalidLedgerName
	}
	return strconv.ParseInt(firstDir+secondDir+name[1:], 10, 64)
}

func getLedgerPathHierarchical(id int64) string {
	var sb strings.Builder
	sb.WriteString("/ledgers/")
	firDirNum := id / 100_000_000
	secDirNum := (id - firDirNum*100_000_000) / 10_000
	remain := id - firDirNum*100_000_000 - secDirNum*10_000
	sb.WriteString(fixedLenStr(firDirNum, 2))
	sb.WriteString("/")
	sb.WriteString(fixedLenStr(secDirNum, 4))
	sb.WriteString("/L")
	sb.WriteString(fixedLenStr(remain, 4))
	return sb.String()
}
