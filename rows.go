package csvdata

import (
	"database/sql"
	"math"
	"strconv"
	"strings"

	"github.com/goark/errs"
)

//RowsReader is interface type for reading columns in a row.
type RowsReader interface {
	Read() ([]string, error)
	Close() error
}

//Rows is a accesser for row-column data set.
type Rows struct {
	reader        RowsReader
	headerFlag    bool
	headerStrings []string
	headerMap     map[string]int
	rowdata       []string
}

func NewRows(rr RowsReader, headerFlag bool) *Rows {
	return &Rows{reader: rr, headerFlag: headerFlag, headerMap: map[string]int{}}
}

//Header method returns header strings.
func (r *Rows) Header() ([]string, error) {
	if r == nil {
		return nil, errs.Wrap(ErrNullPointer)
	}
	var err error
	if r.headerFlag {
		r.headerFlag = false
		r.headerStrings, err = r.reader.Read()
		if len(r.headerStrings) > 0 {
			for i, name := range r.headerStrings {
				r.headerMap[strings.TrimSpace(name)] = i
			}
		}
	}
	return r.headerStrings, errs.Wrap(err)
}

//Next method gets a next record.
func (r *Rows) Next() error {
	if r == nil {
		return errs.Wrap(ErrNullPointer)
	}
	if r.headerFlag {
		if _, err := r.Header(); err != nil {
			return errs.Wrap(err)
		}
	}
	var err error
	r.rowdata, err = r.reader.Read()
	return errs.Wrap(err)
}

//Row method returns current row data.
func (r *Rows) Row() []string {
	if r == nil {
		return nil
	}
	return r.rowdata
}

//GetString method returns string data in current row.
func (r *Rows) GetString(i int) (string, error) {
	if r == nil {
		return "", errs.Wrap(ErrNullPointer)
	}
	if i < 0 || i >= len(r.rowdata) {
		return "", errs.Wrap(ErrOutOfIndex, errs.WithContext("index", i))
	}
	return strings.TrimSpace(r.rowdata[i]), nil
}

//ColumnString method returns string data in current row.
func (r *Rows) ColumnString(s string) (string, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return "", errs.Wrap(err)
	}
	return r.GetString(i)
}

//ColumnNullString method returns ql.NullString data in current row.
func (r *Rows) ColumnNullString(s string) (sql.NullString, error) {
	str, err := r.ColumnString(s)
	if err != nil {
		if errs.Is(err, ErrNullPointer) {
			err = nil
		}
		return sql.NullString{Valid: false}, errs.Wrap(err)
	}
	return sql.NullString{String: str, Valid: len(str) > 0}, nil
}

//GetString method returns string data in current row.
func (r Rows) Get(i int) string {
	s, _ := r.GetString(i)
	return s
}

//GetString method returns string data in current row.
func (r *Rows) Column(s string) string {
	cs, _ := r.ColumnString(s)
	return cs
}

//GetBool method returns type bool data in current row.
func (r *Rows) GetBool(i int) (bool, error) {
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
func (r *Rows) ColumnBool(s string) (bool, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return false, errs.Wrap(err)
	}
	return r.GetBool(i)
}

//ColumnNullBool method returns sql.NullBool data in current row.
func (r *Rows) ColumnNullBool(s string) (sql.NullBool, error) {
	res, err := r.ColumnBool(s)
	if err != nil {
		if errs.Is(err, ErrNullPointer) {
			err = nil
		}
		return sql.NullBool{Valid: false}, errs.Wrap(err)
	}
	return sql.NullBool{Bool: res, Valid: true}, nil
}

//GetFloat method returns type float64 data in current row.
func (r *Rows) GetFloat64(i int) (float64, error) {
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
func (r *Rows) ColumnFloat64(s string) (float64, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return r.GetFloat64(i)
}

//ColumnNullFloat64 method returns sql.NullFloat64 data in current row.
func (r *Rows) ColumnNullFloat64(s string) (sql.NullFloat64, error) {
	res, err := r.ColumnFloat64(s)
	if err != nil {
		if errs.Is(err, ErrNullPointer) {
			err = nil
		}
		return sql.NullFloat64{Valid: false}, errs.Wrap(err)
	}
	return sql.NullFloat64{Float64: res, Valid: true}, nil
}

//GetInt method returns type int64 data in current row.
func (r *Rows) GetInt64(i int, base int) (int64, error) {
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
func (r *Rows) ColumnInt64(s string, base int) (int64, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return r.GetInt64(i, base)
}

//ColumnNullByte method returns sql.NullByte data in current row.
func (r *Rows) ColumnNullByte(s string, base int) (sql.NullByte, error) {
	res, err := r.ColumnNullInt64(s, base)
	if err != nil {
		return sql.NullByte{Valid: false}, errs.Wrap(err)
	}
	if res.Valid && (res.Int64 < 0 || res.Int64 > math.MaxUint8) {
		return sql.NullByte{Valid: false}, errs.Wrap(strconv.ErrRange)
	}
	return sql.NullByte{Byte: byte(res.Int64), Valid: true}, nil
}

//ColumnNullInt16 method returns sql.NullFloat64 data in current row.
func (r *Rows) ColumnNullInt16(s string, base int) (sql.NullInt16, error) {
	res, err := r.ColumnNullInt64(s, base)
	if err != nil {
		return sql.NullInt16{Valid: false}, errs.Wrap(err)
	}
	if res.Valid && (res.Int64 < math.MinInt16 || res.Int64 > math.MaxInt16) {
		return sql.NullInt16{Valid: false}, errs.Wrap(strconv.ErrRange)
	}
	return sql.NullInt16{Int16: int16(res.Int64), Valid: true}, nil
}

//ColumnNullInt32 method returns sql.NullInt32 data in current row.
func (r *Rows) ColumnNullInt32(s string, base int) (sql.NullInt32, error) {
	res, err := r.ColumnNullInt64(s, base)
	if err != nil {
		return sql.NullInt32{Valid: false}, errs.Wrap(err)
	}
	if res.Valid && (res.Int64 < math.MinInt32 || res.Int64 > math.MaxInt32) {
		return sql.NullInt32{Valid: false}, errs.Wrap(strconv.ErrRange)
	}
	return sql.NullInt32{Int32: int32(res.Int64), Valid: true}, nil
}

//ColumnNullInt64 method returns sql.NullInt64 data in current row.
func (r *Rows) ColumnNullInt64(s string, base int) (sql.NullInt64, error) {
	res, err := r.ColumnInt64(s, base)
	if err != nil {
		if errs.Is(err, ErrNullPointer) {
			err = nil
		}
		return sql.NullInt64{Valid: false}, errs.Wrap(err)
	}
	return sql.NullInt64{Int64: res, Valid: true}, nil
}

//Close method is closing RowsReader instance.
func (r *Rows) Close() error {
	return r.reader.Close()
}

func (r *Rows) indexOf(s string) (int, error) {
	if r == nil {
		return 0, errs.Wrap(ErrNullPointer)
	}
	if i, ok := r.headerMap[strings.TrimSpace(s)]; ok {
		return i, nil
	}
	return 0, errs.Wrap(ErrOutOfIndex)
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
