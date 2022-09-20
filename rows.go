package csvdata

import (
	"database/sql"
	"math"
	"strconv"
	"strings"

	"github.com/goark/errs"
)

// RowsReader is interface type for reading columns in a row.
type RowsReader interface {
	Read() ([]string, error)
	Close() error
	LazyQuotes() bool
}

// Rows is a accesser for row-column data set.
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

// LazyQuotes returns LazyQuotes option value.
func (r *Rows) LazyQuotes() bool {
	return r.reader.LazyQuotes()
}

// Header method returns header strings.
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

// Next method gets a next record.
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

// Row method returns current row data.
func (r *Rows) Row() []string {
	if r == nil {
		return nil
	}
	return r.rowdata
}

// GetString method returns string data in current row.
func (r *Rows) GetString(i int) (string, error) {
	if r == nil {
		return "", errs.Wrap(ErrNullValue)
	}
	if i < 0 || i >= len(r.rowdata) {
		return "", errs.Wrap(ErrOutOfIndex, errs.WithContext("index", i))
	}
	s := strings.TrimSpace(r.rowdata[i])
	if r.LazyQuotes() {
		return s, nil
	}
	if len(s) == 0 {
		return "", errs.Wrap(ErrNullValue)
	}
	if ss, err := strconv.Unquote(s); err == nil {
		return ss, nil
	}
	return s, nil
}

// ColumnNullString method returns ql.NullString data in current row.
func (r *Rows) ColumnNullString(s string) (sql.NullString, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return sql.NullString{}, errs.Wrap(err)
	}
	str, err := r.GetString(i)
	if err != nil {
		if !errs.Is(err, ErrNullValue) {
			return sql.NullString{}, errs.Wrap(err)
		}
	}
	if r.LazyQuotes() {
		return sql.NullString{String: str, Valid: len(str) > 0}, nil
	}
	return sql.NullString{String: str, Valid: true}, nil
}

// ColumnString method returns string data in current row.
func (r *Rows) ColumnString(s string) (string, error) {
	str, err := r.ColumnNullString(s)
	if err != nil {
		if errs.Is(err, ErrNullValue) {
			return "", nil
		}
		return "", errs.Wrap(err)
	}
	if str.Valid {
		return str.String, nil
	}
	return "", nil
}

// GetString method returns string data in current row.
func (r Rows) Get(i int) string {
	s, _ := r.GetString(i)
	return s
}

// GetString method returns string data in current row.
func (r *Rows) Column(s string) string {
	cs, _ := r.ColumnString(s)
	return cs
}

// GetBool method returns type bool data in current row.
func (r *Rows) GetBool(i int) (bool, error) {
	s, err := r.GetString(i)
	if err != nil {
		return false, errs.Wrap(err)
	}
	if len(s) == 0 {
		return false, errs.Wrap(ErrNullValue)
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, errs.Wrap(err)
	}
	return b, nil
}

// ColumnNullBool method returns sql.NullBool data in current row.
func (r *Rows) ColumnNullBool(s string) (sql.NullBool, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return sql.NullBool{}, errs.Wrap(err)
	}
	res, err := r.GetBool(i)
	if err != nil && !errs.Is(err, ErrNullValue) {
		return sql.NullBool{}, errs.Wrap(err)
	}
	return sql.NullBool{Bool: res, Valid: err == nil}, nil
}

// ColumnBool method returns type bool data in current row.
func (r *Rows) ColumnBool(s string) (bool, error) {
	res, err := r.ColumnNullBool(s)
	if err != nil {
		return false, errs.Wrap(err)
	}
	if res.Valid {
		return res.Bool, nil
	}
	return false, errs.Wrap(ErrNullValue)
}

// GetFloat method returns type float64 data in current row.
func (r *Rows) GetFloat64(i int) (float64, error) {
	s, err := r.GetString(i)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if len(s) == 0 {
		return 0, errs.Wrap(ErrNullValue)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return f, nil
}

// ColumnNullFloat64 method returns sql.NullFloat64 data in current row.
func (r *Rows) ColumnNullFloat64(s string) (sql.NullFloat64, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return sql.NullFloat64{}, errs.Wrap(err)
	}
	res, err := r.GetFloat64(i)
	if err != nil && !errs.Is(err, ErrNullValue) {
		return sql.NullFloat64{}, errs.Wrap(err)
	}
	return sql.NullFloat64{Float64: res, Valid: err == nil}, nil
}

// ColumnFloat method returns type float64 data in current row.
func (r *Rows) ColumnFloat64(s string) (float64, error) {
	res, err := r.ColumnNullFloat64(s)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if res.Valid {
		return res.Float64, nil
	}
	return 0, errs.Wrap(ErrNullValue)
}

// GetInt method returns type int64 data in current row.
func (r *Rows) GetInt64(i int, base int) (int64, error) {
	s, err := r.GetString(i)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if len(s) == 0 {
		return 0, errs.Wrap(ErrNullValue)
	}
	n, err := strconv.ParseInt(s, base, 64)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return n, nil
}

// ColumnNullInt64 method returns sql.NullInt64 data in current row.
func (r *Rows) ColumnNullInt64(s string, base int) (sql.NullInt64, error) {
	i, err := r.indexOf(s)
	if err != nil {
		return sql.NullInt64{}, errs.Wrap(err)
	}
	res, err := r.GetInt64(i, base)
	if err != nil && !errs.Is(err, ErrNullValue) {
		return sql.NullInt64{}, errs.Wrap(err)
	}
	return sql.NullInt64{Int64: res, Valid: err == nil}, nil
}

// ColumnNullInt32 method returns sql.NullInt32 data in current row.
func (r *Rows) ColumnNullInt32(s string, base int) (sql.NullInt32, error) {
	res, err := r.ColumnNullInt64(s, base)
	if err != nil {
		return sql.NullInt32{}, errs.Wrap(err)
	}
	if res.Valid && (res.Int64 < math.MinInt32 || res.Int64 > math.MaxInt32) {
		return sql.NullInt32{}, errs.Wrap(strconv.ErrRange)
	}
	return sql.NullInt32{Int32: int32(res.Int64 & 0xffffffff), Valid: true}, nil
}

// ColumnNullInt16 method returns sql.NullFloat64 data in current row.
func (r *Rows) ColumnNullInt16(s string, base int) (sql.NullInt16, error) {
	res, err := r.ColumnNullInt64(s, base)
	if err != nil {
		return sql.NullInt16{Valid: false}, errs.Wrap(err)
	}
	if res.Valid && (res.Int64 < math.MinInt16 || res.Int64 > math.MaxInt16) {
		return sql.NullInt16{Valid: false}, errs.Wrap(strconv.ErrRange)
	}
	return sql.NullInt16{Int16: int16(res.Int64 & 0xffff), Valid: true}, nil
}

// ColumnNullByte method returns sql.NullByte data in current row.
func (r *Rows) ColumnNullByte(s string, base int) (sql.NullByte, error) {
	res, err := r.ColumnNullInt64(s, base)
	if err != nil {
		return sql.NullByte{Valid: false}, errs.Wrap(err)
	}
	if res.Valid && (res.Int64 < 0 || res.Int64 > math.MaxUint8) {
		return sql.NullByte{Valid: false}, errs.Wrap(strconv.ErrRange)
	}
	return sql.NullByte{Byte: byte(res.Int64 & 0xff), Valid: true}, nil
}

// ColumnInt64 method returns type int64 data in current row.
func (r *Rows) ColumnInt64(s string, base int) (int64, error) {
	res, err := r.ColumnNullInt64(s, base)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if res.Valid {
		return res.Int64, nil
	}
	return 0, errs.Wrap(ErrNullValue)
}

// ColumnInt32 method returns type int32 data in current row.
func (r *Rows) ColumnInt32(s string, base int) (int32, error) {
	res, err := r.ColumnNullInt32(s, base)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if res.Valid {
		return res.Int32, nil
	}
	return 0, errs.Wrap(ErrNullValue)
}

// ColumnInt16 method returns type int16 data in current row.
func (r *Rows) ColumnInt16(s string, base int) (int16, error) {
	res, err := r.ColumnNullInt16(s, base)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if res.Valid {
		return res.Int16, nil
	}
	return 0, errs.Wrap(ErrNullValue)
}

// ColumnInt8 method returns type int8 data in current row.
func (r *Rows) ColumnInt8(s string, base int) (int8, error) {
	res, err := r.ColumnNullInt16(s, base)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if res.Valid {
		if res.Int16 < math.MinInt8 || res.Int16 > math.MaxInt8 {
			return 0, errs.Wrap(strconv.ErrRange)
		}
		return int8(res.Int16 & 0xff), nil
	}
	return 0, errs.Wrap(ErrNullValue)
}

// ColumnInt16 method returns type int16 data in current row.
func (r *Rows) ColumnByte(s string, base int) (byte, error) {
	res, err := r.ColumnNullByte(s, base)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	if res.Valid {
		return res.Byte, nil
	}
	return 0, errs.Wrap(ErrNullValue)
}

// Close method is closing RowsReader instance.
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
