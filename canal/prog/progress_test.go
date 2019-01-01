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

package prog

import (
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEqual(t *testing.T) {
	p1 := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1001,
		},
	}
	p2 := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1001,
		},
	}
	require.Equal(t, p1, p2)
}

func TestLessThan(t *testing.T) {
	p1 := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1000,
		},
	}
	p2 := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1001,
		},
	}
	require.Equal(t, -1, p1.Compare(&p2))
}

func TestLargeThan(t *testing.T) {
	p1 := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1002,
		},
	}
	p2 := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1001,
		},
	}
	require.Equal(t, 1, p1.Compare(&p2))
}

func TestZero(t *testing.T) {
	p1 := Progress{}
	require.True(t, p1.IsZero())

	p := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1001,
		},
	}
	require.False(t, p.IsZero())

	gset, err := mysql.ParseMysqlGTIDSet("cb477437-aacb-11e8-90e7-0242ac110002:1-10")
	require.Nil(t, err)
	p = Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1001,
		},
		Gset: gset,
	}
	require.False(t, p.IsZero())

	// if Gset != nil, but Gset.string == "", we treat it as zero
	gset, err = mysql.ParseMysqlGTIDSet("")
	require.Nil(t, err)
	p = Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1001,
		},
		Gset: gset,
	}
	require.True(t, p.IsZero())

	p = Progress{
		Pos: Position{
			Name: "",
			Pos:  0,
		},
		Gset: nil,
	}
	require.True(t, p.IsZero())

}

func TestCopy(t *testing.T) {
	gset, err := mysql.ParseMysqlGTIDSet("cb477437-aacb-11e8-90e7-0242ac110002:1-10")
	require.Nil(t, err)
	p1 := Progress{
		Pos: Position{
			Name: "mysql-bin.000002",
			Pos:  1002,
		},
		Gset: gset,
	}

	p2 := p1
	require.Equal(t, p1, p2)

	p1.Update(Position{Name: "mysql-bin.000003", Pos: 4})
	require.NotEqual(t, p1, p2)

	p2 = p1
	require.Equal(t, p1, p2)

	gset, err = mysql.ParseMysqlGTIDSet("cb477437-aacb-11e8-90e7-0242ac110002:1-11")
	require.Nil(t, err)
	p1.UpdateGTIDSet(gset)
	require.NotEqual(t, p1, p2)

	p2 = p1
	require.Equal(t, p1, p2)

}
