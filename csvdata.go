package csvdata

import (
	"encoding/csv"
	"io"
	"strconv"
	"strings"

	"github.com/spiegel-im-spiegel/errs"
)

//Reader is class of CSV reader
type Reader struct {
	reader        *csv.Reader
	headerFlag    bool
	headerStrings []string
	rowdata       []string
}

//New function creates a new Reader instance.
func New(r io.Reader, headerFlag bool) *Reader {
	cr := csv.NewReader(r)
	cr.Comma = ','
	cr.LazyQuotes = true       // a quote may appear in an unquoted field and a non-doubled quote may appear in a quoted field.
	cr.TrimLeadingSpace = true // leading
	return &Reader{reader: cr, headerFlag: headerFlag}
}

//WithComma method sets Comma property.
func (r *Reader) WithComma(c rune) *Reader {
	if r == nil {
		return nil
	}
	r.reader.Comma = c
	return r
}

//WithFieldsPerRecord method sets FieldsPerRecord property.
func (r *Reader) WithFieldsPerRecord(size int) *Reader {
	if r == nil {
		return nil
	}
	r.reader.FieldsPerRecord = size
	return r
}

//Header method returns header strings.
func (r *Reader) Header() ([]string, error) {
	if r == nil {
		return nil, errs.Wrap(ErrNullPointer)
	}
	var err error
	if r.headerFlag {
		r.headerFlag = false
		r.headerStrings, err = r.readRecord()
	}
	return r.headerStrings, errs.Wrap(err)
}

//Next method gets a next record.
func (r *Reader) Next() error {
	if r == nil {
		return errs.Wrap(ErrNullPointer)
	}
	if r.headerFlag {
		if _, err := r.Header(); err != nil {
			return errs.Wrap(err)
		}
	}
	var err error
	r.rowdata, err = r.readRecord()
	return errs.Wrap(err)
}

//Row method returns current row data.
func (r *Reader) Row() []string {
	if r == nil {
		return nil
	}
	return r.rowdata
}

//GetString method returns string data in current row.
func (r *Reader) Get(i int) string {
	s, _ := r.GetString(i)
	return s
}

//GetString method returns string data in current row.
func (r *Reader) Column(s string) string {
	cs, _ := r.ColumnString(s)
	return cs
}

//GetString method returns string data in current row.
func (r *Reader) GetString(i int) (string, error) {
	if r == nil {
		return "", errs.Wrap(ErrNullPointer)
	}
	if i < 0 || i >= len(r.rowdata) {
		return "", errs.Wrap(ErrOutOfIndex, errs.WithContext("index", i))
	}
	return r.rowdata[i], nil
}

//ColumnString method returns string data in current row.
func (r *Reader) ColumnString(s string) (string, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return "", errs.Wrap(err)
	}
	return r.GetString(i)
}

//GetBool method returns type bool data in current row.
func (r *Reader) GetBool(i int) (bool, error) {
	s, err := r.GetString(i)
	if err != nil {
		return false, errs.Wrap(err)
	}
	if len(s) == 0 {
		return false, errs.Wrap(ErrNullPointer)
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, errs.Wrap(err)
	}
	return b, nil
}

//ColumnBool method returns type bool data in current row.
func (r *Reader) ColumnBool(s string) (bool, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return false, errs.Wrap(err)
	}
	return r.GetBool(i)
}

//GetFloat method returns type float64 data in current row.
func (r *Reader) GetFloat64(i int) (float64, error) {
	s, err := r.GetString(i)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if len(s) == 0 {
		return 0, errs.Wrap(ErrNullPointer)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return f, nil
}

//ColumnFloat method returns type float64 data in current row.
func (r *Reader) ColumnFloat64(s string) (float64, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return r.GetFloat64(i)
}

//GetInt method returns type int64 data in current row.
func (r *Reader) GetInt64(i int, base int) (int64, error) {
	s, err := r.GetString(i)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if len(s) == 0 {
		return 0, errs.Wrap(ErrNullPointer)
	}
	n, err := strconv.ParseInt(s, base, 64)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return n, nil
}

//ColumnInt method returns type int64 data in current row.
func (r *Reader) ColumnInt64(s string, base int) (int64, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return r.GetInt64(i, base)
}

func (r *Reader) readRecord() ([]string, error) {
	elms, err := r.reader.Read()
	if err != nil {
		if errs.Is(err, io.EOF) {
			return nil, errs.Wrap(err)
		}
		return nil, errs.Wrap(ErrInvalidRecord, errs.WithCause(err))
	}
	return elms, nil
}

func (r *Reader) indexOf(s string) (int, error) {
	if r == nil {
		return 0, errs.Wrap(ErrNullPointer)
	}
	if len(r.headerStrings) == 0 {
		return 0, errs.Wrap(ErrOutOfIndex)
	}
	for i, name := range r.headerStrings {
		if strings.EqualFold(s, name) {
			return i, nil
		}
	}
	return 0, errs.Wrap(ErrOutOfIndex)
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
