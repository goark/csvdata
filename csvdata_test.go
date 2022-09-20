package csvdata_test

import (
	"database/sql"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/goark/csvdata"
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
	r := csvdata.NewRows((*csvdata.Reader)(nil).WithComma(',').WithLazyQuotes(true).WithTrimLeadingSpace(true).WithFieldsPerRecord(1), true)
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
	r := csvdata.NewRows(csvdata.New(iotest.ErrReader(errtest)).WithComma(',').WithLazyQuotes(true).WithTrimLeadingSpace(true).WithFieldsPerRecord(1), true)
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
		sep         rune
		size        int
		headerFlag  bool
		inp         io.Reader
		name1       string
		name2       string
		flag        bool
		flagBool    sql.NullBool
		mass        float64
		massFloat64 sql.NullFloat64
		order       int8
		orderByte   sql.NullByte
		orderInt16  sql.NullInt16
		orderInt32  sql.NullInt32
		orderInt64  sql.NullInt64
		err         error
	}{
		{sep: ',', size: 6, headerFlag: true, inp: strings.NewReader(csv1), name1: "Mercury", name2: "Mercury", flag: false, flagBool: sql.NullBool{Bool: false, Valid: true}, mass: 0.055, massFloat64: sql.NullFloat64{Float64: 0.055, Valid: true}, order: 1, orderByte: sql.NullByte{Byte: 1, Valid: true}, orderInt16: sql.NullInt16{Int16: 1, Valid: true}, orderInt32: sql.NullInt32{Int32: 1, Valid: true}, orderInt64: sql.NullInt64{Int64: 1, Valid: true}, err: nil},
		{sep: '\t', size: 6, headerFlag: false, inp: strings.NewReader(tsv1), name1: "Mercury", name2: "", flag: false, flagBool: sql.NullBool{Bool: false, Valid: true}, mass: 0.055, massFloat64: sql.NullFloat64{Float64: 0.055, Valid: true}, order: 1, orderByte: sql.NullByte{Byte: 1, Valid: true}, orderInt16: sql.NullInt16{Int16: 1, Valid: true}, orderInt32: sql.NullInt32{Int32: 1, Valid: true}, orderInt64: sql.NullInt64{Int64: 1, Valid: true}, err: csvdata.ErrOutOfIndex},
	}

	for _, tc := range testCases {
		rc := csvdata.NewRows(csvdata.New(tc.inp).WithComma(tc.sep).WithLazyQuotes(true).WithTrimLeadingSpace(true).WithFieldsPerRecord(tc.size), tc.headerFlag)
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
			if _, err = rc.GetBool(5); !errors.Is(err, csvdata.ErrNullValue) {
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
			flagBool, err := rc.ColumnNullBool("habitable")
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnBool() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && flagBool != tc.flagBool {
				t.Errorf("ColumnBool() is \"%+v\", want \"%+v\".", flagBool, tc.flagBool)
			}
			//float
			if _, err = rc.GetFloat64(5); !errors.Is(err, csvdata.ErrNullValue) {
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
			massFloat64, err := rc.ColumnNullFloat64("mass")
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnNullFloat64() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && massFloat64 != tc.massFloat64 {
				t.Errorf("ColumnNullFloat64() is \"%+v\", want \"%+v\".", massFloat64, tc.massFloat64)
			}
			//int
			if _, err = rc.GetInt64(5, 10); !errors.Is(err, csvdata.ErrNullValue) {
				t.Errorf("GetInt64() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			if _, err = rc.ColumnInt64("name", 10); !errors.Is(err, strconv.ErrSyntax) && !errors.Is(err, tc.err) {
				t.Errorf("ColumnInt64() is \"%+v\", want \"%+v\".", err, strconv.ErrSyntax)
			}
			order64, err := rc.ColumnInt64("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnInt64() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && order64 != int64(tc.order) {
				t.Errorf("ColumnInt64() is \"%+v\", want \"%+v\".", order64, tc.order)
			}
			order32, err := rc.ColumnInt32("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnInt32() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && order32 != int32(tc.order) {
				t.Errorf("ColumnInt32() is \"%+v\", want \"%+v\".", order32, tc.order)
			}
			order16, err := rc.ColumnInt16("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnInt16() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && order16 != int16(tc.order) {
				t.Errorf("ColumnInt16() is \"%+v\", want \"%+v\".", order16, tc.order)
			}
			order8, err := rc.ColumnInt8("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnInt8() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && order8 != tc.order {
				t.Errorf("ColumnInt8() is \"%+v\", want \"%+v\".", order8, tc.order)
			}
			orderB, err := rc.ColumnByte("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnByte() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && orderB != byte(tc.order) {
				t.Errorf("ColumnByte() is \"%+v\", want \"%+v\".", orderB, tc.order)
			}
			orderByte, err := rc.ColumnNullByte("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnNullByte() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && orderByte != tc.orderByte {
				t.Errorf("ColumnNullByte() is \"%+v\", want \"%+v\".", orderByte, tc.orderByte)
			}
			orderInt16, err := rc.ColumnNullInt16("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnNullInt16() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && orderInt16 != tc.orderInt16 {
				t.Errorf("ColumnNullInt16() is \"%+v\", want \"%+v\".", orderInt16, tc.orderInt16)
			}
			orderInt32, err := rc.ColumnNullInt32("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnNullInt32() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && orderInt32 != tc.orderInt32 {
				t.Errorf("ColumnNullInt32() is \"%+v\", want \"%+v\".", orderInt32, tc.orderInt32)
			}
			orderInt64, err := rc.ColumnNullInt64("order", 10)
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnNullInt64() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && orderInt64 != tc.orderInt64 {
				t.Errorf("ColumnNullInt64() is \"%+v\", want \"%+v\".", orderInt64, tc.orderInt64)
			}
		}
	}
}

/* Copyright 2021-2022 Spiegel
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
