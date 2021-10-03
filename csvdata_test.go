package csvdata_test

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/spiegel-im-spiegel/csvdata"
)

const (
	csv1 = `"order", name ,"mass","distance","habitable","note"
1, Mercury, 0.055, 0.4,false,""
2, Venus, 0.815, 0.7,false,""
3, Earth, 1.0, 1.0,true,""
4, Mars, 0.107, 1.5,false,""
`
	tsv1 = `1	 Mercury	 0.055	 0.4	false	""
2	 Venus	 0.815	 0.7	false	""
3	 Earth	 1.0	 1.0	true	""
4	 Mars	 0.107	 1.5	false	""
`
)

func TestWithNil(t *testing.T) {
	r := csvdata.NewRows((*csvdata.Reader)(nil).WithComma(',').WithFieldsPerRecord(1), true)
	defer r.Close() //dummy
	if err := r.Next(); !errors.Is(err, csvdata.ErrNullPointer) {
		t.Errorf("Next() is \"%+v\", want \"%+v\".", err, csvdata.ErrNullPointer)
	}
	if _, err := r.Header(); err != nil {
		t.Errorf("Header() is \"%+v\", want <nil>.", err)
	}
	//Row
	if row := r.Row(); row != nil {
		t.Errorf("Row() is not nil, want nil.")
	}
	//string
	if _, err := r.GetString(0); !errors.Is(err, csvdata.ErrOutOfIndex) {
		t.Errorf("GetString() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
	}
	if s := r.Get(0); s != "" {
		t.Errorf("Get() is \"%v\", want \"\".", s)
	}
	if s := r.Column("foo"); s != "" {
		t.Errorf("Get() is \"%v\", want \"\".", s)
	}
	//bool
	if _, err := r.GetBool(0); !errors.Is(err, csvdata.ErrOutOfIndex) {
		t.Errorf("GetBool() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
	}
	//float
	if _, err := r.GetFloat64(0); !errors.Is(err, csvdata.ErrOutOfIndex) {
		t.Errorf("GetFloat() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
	}
	//int
	if _, err := r.GetInt64(0, 10); !errors.Is(err, csvdata.ErrOutOfIndex) {
		t.Errorf("GetFloat() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
	}
}

func TestErrReader(t *testing.T) {
	errtest := errors.New("test")
	r := csvdata.NewRows(csvdata.New(iotest.ErrReader(errtest)).WithComma(',').WithFieldsPerRecord(1), true)
	defer r.Close() //dummy
	if err := r.Next(); !errors.Is(err, errtest) {
		t.Errorf("Next() is \"%+v\", want \"%+v\".", err, errtest)
	}
	if _, err := r.GetString(0); !errors.Is(err, csvdata.ErrOutOfIndex) {
		t.Errorf("GetString() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
	}
}

func TestBlankReader(t *testing.T) {
	r := csvdata.NewRows(csvdata.New(strings.NewReader("")).WithComma(',').WithFieldsPerRecord(1), true)
	defer r.Close() //dummy
	if err := r.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("Next() is \"%+v\", want \"%+v\".", err, io.EOF)
	}
}

func TestNormal(t *testing.T) {
	testCases := []struct {
		sep        rune
		size       int
		headerFlag bool
		inp        io.Reader
		name1      string
		name2      string
		flag       bool
		mass       float64
		order      int64
		err        error
	}{
		{sep: ',', size: 6, headerFlag: true, inp: strings.NewReader(csv1), name1: "Mercury", name2: "Mercury", flag: false, mass: 0.055, order: 1, err: nil},
		{sep: '\t', size: 6, headerFlag: false, inp: strings.NewReader(tsv1), name1: "Mercury", name2: "", flag: false, mass: 0.055, order: 1, err: csvdata.ErrOutOfIndex},
	}

	for _, tc := range testCases {
		rc := csvdata.NewRows(csvdata.New(tc.inp).WithComma(tc.sep).WithFieldsPerRecord(tc.size), tc.headerFlag)
		if err := rc.Next(); err != nil {
			t.Errorf("Next() is \"%+v\", want nil.", err)
		} else {
			//Size
			if size := len(rc.Row()); size != tc.size {
				t.Errorf("Size of Row() is %v, want %+v.", size, tc.size)
			}
			//index
			if _, err = rc.GetString(-1); !errors.Is(err, csvdata.ErrOutOfIndex) {
				t.Errorf("GetString() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
			}
			if _, err = rc.GetString(tc.size); !errors.Is(err, csvdata.ErrOutOfIndex) {
				t.Errorf("GetString() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
			}
			//string
			if _, err = rc.ColumnString("foo"); !errors.Is(err, csvdata.ErrOutOfIndex) {
				t.Errorf("ColumnString() is \"%+v\", want \"%+v\".", err, csvdata.ErrOutOfIndex)
			}
			name, err := rc.ColumnString("name")
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnString() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && name != tc.name1 {
				t.Errorf("ColumnString() is \"%+v\", want \"%+v\".", name, tc.name1)
			}
			if name = rc.Get(1); name != tc.name1 {
				t.Errorf("Get() is \"%v\", want \"%v\".", name, tc.name1)
			}
			if name = rc.Column("name"); name != tc.name2 {
				t.Errorf("Column() is \"%v\", want \"%v\".", name, tc.name2)
			}
			//bool
			if _, err = rc.GetBool(5); !errors.Is(err, csvdata.ErrNullPointer) {
				t.Errorf("GetBool() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			if _, err = rc.ColumnBool("name"); !errors.Is(err, strconv.ErrSyntax) && !errors.Is(err, tc.err) {
				t.Errorf("ColumnBool() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			flag, err := rc.ColumnBool("habitable")
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnBool() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && flag != tc.flag {
				t.Errorf("ColumnBool() is \"%+v\", want \"%+v\".", flag, tc.flag)
			}
			//float
			if _, err = rc.GetFloat64(5); !errors.Is(err, csvdata.ErrNullPointer) {
				t.Errorf("GetFloat() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			if _, err = rc.ColumnFloat64("name"); !errors.Is(err, strconv.ErrSyntax) && !errors.Is(err, tc.err) {
				t.Errorf("ColumnFloat() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			mass, err := rc.ColumnFloat64("mass")
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnFloat() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && mass != tc.mass {
				t.Errorf("ColumnFloat() is \"%+v\", want \"%+v\".", mass, tc.mass)
			}
			//int
			if _, err = rc.GetInt64(5, 10); !errors.Is(err, csvdata.ErrNullPointer) {
				t.Errorf("GetFloat() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			if _, err = rc.ColumnInt64("name", 10); !errors.Is(err, strconv.ErrSyntax) && !errors.Is(err, tc.err) {
				t.Errorf("ColumnFloat() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			order, err := rc.ColumnInt64("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnFloat() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && order != tc.order {
				t.Errorf("ColumnFloat() is \"%+v\", want \"%+v\".", order, tc.order)
			}
		}
	}
}

/* Copyright 2021 Spiegel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
