package csvdata_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/spiegel-im-spiegel/csvdata"
)

const (
	csv1 = `"order","name","mass","distance","habitable"
1, Mercury, 0.055, 0.4,false
2, Venus, 0.815, 0.7,false
3, Earth, 1.0, 1.0,true
4, Mars, 0.107, 1.5,false
`
	tsv1 = `1	 Mercury	 0.055	 0.4	false
2	 Venus	 0.815	 0.7	false
3	 Earth	 1.0	 1.0	true
4	 Mars	 0.107	 1.5	false
`
)

func TestWithNil(t *testing.T) {
	r := (*csvdata.Reader)(nil).WithComma(',').WithFieldsPerRecord(1)
	if err := r.Next(); !errors.Is(err, csvdata.ErrNullPointer) {
		t.Errorf("Next() is \"%+v\", want \"%+v\".", err, csvdata.ErrNullPointer)
	}
	if _, err := r.Header(); !errors.Is(err, csvdata.ErrNullPointer) {
		t.Errorf("Header() is \"%+v\", want \"%+v\".", err, csvdata.ErrNullPointer)
	}
	if row := r.Row(); r != nil {
		t.Errorf("Row() is \"%v\", want nil.", row)
	}
	if _, err := r.GetString(0); !errors.Is(err, csvdata.ErrNullPointer) {
		t.Errorf("GetString() is \"%+v\", want \"%+v\".", err, csvdata.ErrNullPointer)
	}
	if _, err := r.GetString(0); !errors.Is(err, csvdata.ErrNullPointer) {
		t.Errorf("ColumnString() is \"%+v\", want \"%+v\".", err, csvdata.ErrNullPointer)
	}
}

func TestWithComma(t *testing.T) {
	testCases := []struct {
		sep        rune
		size       int
		headerFlag bool
		inp        io.Reader
		name       string
		err        error
	}{
		{sep: ',', size: 5, headerFlag: true, inp: strings.NewReader(csv1), name: "Mercury", err: nil},
		{sep: '\t', size: 5, headerFlag: false, inp: strings.NewReader(tsv1), name: "", err: csvdata.ErrOutOfIndex},
	}

	for _, tc := range testCases {
		rc := csvdata.New(tc.inp, tc.headerFlag).WithComma(tc.sep).WithFieldsPerRecord(tc.size)
		if err := rc.Next(); err != nil {
			t.Errorf("Next() is \"%+v\", want nil.", err)
		} else {
			name, err := rc.ColumnString("NAME")
			if !errors.Is(err, tc.err) {
				t.Errorf("ColumnString() is \"%+v\", want \"%+v\".", err, tc.err)
			}
			if err == nil && name != tc.name {
				t.Errorf("ColumnString() is \"%+v\", want \"%+v\".", name, tc.name)
			}
			if name = rc.Column("name"); name != tc.name {
				t.Errorf("Column() is \"%v\", want \"%v\".", name, tc.name)
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
