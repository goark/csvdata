package calcdata

import (
	"bytes"
	"io"

	"github.com/goark/csvdata"
	"github.com/goark/errs"
	"github.com/knieriem/odf/ods"
)

// Reader is class of LibreOffice Calc data
type Reader struct {
	table          *ods.Table
	offset, repeat int
}

var _ csvdata.RowsReader = (*Reader)(nil) //Reader is compatible with csvdata.RowsReader interface

// OpenFile returns Calc file instance.
func OpenFile(path string) (*ods.Doc, error) {
	doc, err := openFile(path)
	if err != nil {
		return nil, errs.Wrap(err, errs.WithContext("path", path))
	}
	return doc, nil
}

// New function creates a new Reader instance.
func New(doc *ods.Doc, sheetName string) (*Reader, error) {
	index := sheetIndex(doc, sheetName)
	if index < 0 {
		return nil, errs.Wrap(csvdata.ErrInvalidSheetName, errs.WithContext("sheetName", sheetName))
	}
	return &Reader{table: &doc.Table[index]}, nil
}

// TrimSpace returns false.
func (r *Reader) TrimSpace() bool {
	return false
}

// LazyQuotes returns true.
func (r *Reader) LazyQuotes() bool {
	return true
}

func (r *Reader) Read() ([]string, error) {
	if r == nil || r.table == nil {
		return nil, errs.Wrap(csvdata.ErrNullPointer)
	}
	if r.offset >= len(r.table.Row) {
		return nil, errs.Wrap(io.EOF)
	}
	row := r.table.Row[r.offset]
	cols := row.Strings(&bytes.Buffer{})
	r.repeat++
	if r.repeat >= row.RepeatedRows {
		r.offset++
		r.repeat = 0
	}
	return cols, nil
}

func openFile(path string) (*ods.Doc, error) {
	f, err := ods.Open(path)
	if err != nil {
		return nil, errs.Wrap(err, errs.WithContext("path", path))
	}
	defer f.Close()
	var doc ods.Doc
	if err := f.ParseContent(&doc); err != nil {
		return nil, errs.Wrap(err, errs.WithContext("path", path))
	}
	return &doc, nil
}

func sheetIndex(doc *ods.Doc, s string) int {
	if len(s) == 0 {
		return 0
	}
	if doc == nil || len(doc.Table) == 0 {
		return -1
	}
	for i, table := range doc.Table {
		if s == table.Name {
			return i
		}
	}
	return -1
}

// Close method is dummy.
func (r *Reader) Close() error {
	return nil
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
