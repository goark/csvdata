package csvdata

import (
	"encoding/csv"
	"io"

	"github.com/spiegel-im-spiegel/errs"
)

//Reader is class of CSV reader
type Reader struct {
	reader *csv.Reader
}

//New function creates a new Reader instance.
func New(r io.Reader) *Reader {
	cr := csv.NewReader(r)
	cr.Comma = ','
	cr.LazyQuotes = true       // a quote may appear in an unquoted field and a non-doubled quote may appear in a quoted field.
	cr.TrimLeadingSpace = true // leading
	return &Reader{cr}
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

//Read method returns next row data.
func (r *Reader) Read() ([]string, error) {
	if r == nil {
		return nil, errs.Wrap(ErrNullPointer)
	}
	elms, err := r.reader.Read()
	if err != nil {
		if errs.Is(err, io.EOF) {
			return nil, errs.Wrap(err)
		}
		return nil, errs.Wrap(ErrInvalidRecord, errs.WithCause(err))
	}
	return elms, nil
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
