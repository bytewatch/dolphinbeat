// Copyright 2019 siddontang All Rights Reserved.
//
// Licensed under the MIT License;
// License can be found in the LICENSES/go-mysql-LICENSE.
//
// Copyright 2019 ByteWatch All Rights Reserved.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dump

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
)

var (
	ErrSkip = errors.New("Handler error, but skipped")
)

type ParseHandler interface {
	// Parse CHANGE MASTER TO MASTER_LOG_FILE=name, MASTER_LOG_POS=pos;
	BinLog(name string, pos uint64) error
	Gtid(gtid string) error
	DDL(schema string, statement string) error
	Data(schema string, table string, values []string) error
}

var binlogExp *regexp.Regexp
var gtidExp1 *regexp.Regexp
var gtidExp2 *regexp.Regexp
var useExp *regexp.Regexp
var ddlExp *regexp.Regexp
var valuesExp *regexp.Regexp

func init() {
	binlogExp = regexp.MustCompile("^CHANGE MASTER TO MASTER_LOG_FILE='(.+)', MASTER_LOG_POS=(\\d+);")
	gtidExp1 = regexp.MustCompile("^SET @@GLOBAL.GTID_PURGED='(.+)';")
	gtidExp2 = regexp.MustCompile("SET GLOBAL gtid_slave_pos='(.+)';")
	useExp = regexp.MustCompile("^USE `(.+)`;")
	ddlExp = regexp.MustCompile("^CREATE\\s.*")
	valuesExp = regexp.MustCompile("^INSERT INTO `(.+?)` VALUES \\((.+)\\);$")
}

// Parse the dump data with Dumper generate.
// It can not parse all the data formats with mysqldump outputs
func Parse(r io.Reader, h ParseHandler, parseBinlogPos bool, parseGtidSet bool) error {
	rb := bufio.NewReaderSize(r, 1024*16)

	var db string
	var gtidParsed bool
	var binlogParsed bool
	sql := ""
	for {
		line, err := rb.ReadString('\n')
		if err != nil && err != io.EOF {
			return errors.Trace(err)
		} else if mysql.ErrorEqual(err, io.EOF) {
			break
		}

		// Ignore '\n' on Linux or '\r\n' on Windows
		line = strings.TrimRightFunc(line, func(c rune) bool {
			return c == '\r' || c == '\n'
		})

		sql = sql + line
		if line == "" || line[len(line)-1] != ';' {
			continue
		}

		if parseGtidSet && !gtidParsed {
			m := gtidExp1.FindAllStringSubmatch(sql, -1)
			if len(m) != 1 {
				m = gtidExp2.FindAllStringSubmatch(sql, -1)
			}
			if len(m) == 1 {
				gtid := m[0][1]
				if err = h.Gtid(gtid); err != nil && err != ErrSkip {
					return errors.Trace(err)
				}
				gtidParsed = true
			}
		}

		if parseBinlogPos && !binlogParsed {
			if m := binlogExp.FindAllStringSubmatch(sql, -1); len(m) == 1 {
				name := m[0][1]
				pos, err := strconv.ParseUint(m[0][2], 10, 64)
				if err != nil {
					return errors.Errorf("parse binlog %v err, invalid number", sql)
				}

				if err = h.BinLog(name, pos); err != nil && err != ErrSkip {
					return errors.Trace(err)
				}

				binlogParsed = true
			}
		}

		if m := useExp.FindAllStringSubmatch(sql, -1); len(m) == 1 {
			db = m[0][1]
		}

		if m := ddlExp.FindAllStringSubmatch(sql, -1); len(m) == 1 {
			if err = h.DDL(db, sql); err != nil {
				return errors.Trace(err)
			}
		}

		if m := valuesExp.FindAllStringSubmatch(sql, -1); len(m) == 1 {
			table := m[0][1]

			values, err := parseValues(m[0][2])
			if err != nil {
				return errors.Errorf("parse values %v err", sql)
			}

			if err = h.Data(db, table, values); err != nil && err != ErrSkip {
				return errors.Trace(err)
			}
		}
		sql = ""
	}

	return nil
}

func parseValues(str string) ([]string, error) {
	// values are seperated by comma, but we can not split using comma directly
	// string is enclosed by single quote

	// a simple implementation, may be more robust later.

	values := make([]string, 0, 8)

	i := 0
	for i < len(str) {
		if str[i] != '\'' {
			// no string, read until comma
			j := i + 1
			for ; j < len(str) && str[j] != ','; j++ {
			}
			values = append(values, str[i:j])
			// skip ,
			i = j + 1
		} else {
			// read string until another single quote
			j := i + 1

			escaped := false
			for j < len(str) {
				if str[j] == '\\' {
					// skip escaped character
					j += 2
					escaped = true
					continue
				} else if str[j] == '\'' {
					break
				} else {
					j++
				}
			}

			if j >= len(str) {
				return nil, fmt.Errorf("parse quote values error")
			}

			value := str[i : j+1]
			if escaped {
				value = unescapeString(value)
			}
			values = append(values, value)
			// skip ' and ,
			i = j + 2
		}

		// need skip blank???
	}

	return values, nil
}

// unescapeString un-escapes the string.
// mysqldump will escape the string when dumps,
// Refer http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
func unescapeString(s string) string {
	i := 0

	value := make([]byte, 0, len(s))
	for i < len(s) {
		if s[i] == '\\' {
			j := i + 1
			if j == len(s) {
				// The last char is \, remove
				break
			}

			value = append(value, unescapeChar(s[j]))
			i += 2
		} else {
			value = append(value, s[i])
			i++
		}
	}

	return string(value)
}

func unescapeChar(ch byte) byte {
	// \" \' \\ \n \0 \b \Z \r \t ==> escape to one char
	switch ch {
	case 'n':
		ch = '\n'
	case '0':
		ch = 0
	case 'b':
		ch = 8
	case 'Z':
		ch = 26
	case 'r':
		ch = '\r'
	case 't':
		ch = '\t'
	}
	return ch
}
