package exceldata

import (
	"io"

	"github.com/goark/csvdata"
	"github.com/goark/errs"
	"github.com/xuri/excelize/v2"
)

//Reader is class of Excel data
type Reader struct {
	rows *excelize.Rows
}

var _ csvdata.RowsReader = (*Reader)(nil) //Reader is compatible with csvdata.RowsReader interface

//OpenFile returns Excel file instance.
func OpenFile(path, password string) (*excelize.File, error) {
	xlsx, err := excelize.OpenFile(path, excelize.Options{Password: password})
	if err != nil {
		return xlsx, errs.Wrap(err, errs.WithContext("path", path))
	}
	return xlsx, nil
}

//New function creates a new Reader instance.
func New(xlsx *excelize.File, sheetName string) (*Reader, error) {
	if len(sheetName) == 0 {
		sheetName = xlsx.GetSheetName(0)
	}
	rows, err := xlsx.Rows(sheetName)
	if err != nil {
		var errSheet excelize.ErrSheetNotExist
		if errs.As(err, &errSheet) {
			return nil, errs.Wrap(csvdata.ErrInvalidSheetName, errs.WithCause(err), errs.WithContext("SheetName", errSheet.SheetName))
		}
		return nil, errs.Wrap(err)
	}
	return &Reader{rows}, nil
}

//Read method returns next row data.
func (r *Reader) Read() ([]string, error) {
	if r == nil {
		return nil, errs.Wrap(csvdata.ErrNullPointer)
	}
	if r.rows.Next() {
		cols, err := r.rows.Columns()
		return cols, errs.Wrap(err)
	}
	if err := r.rows.Error(); err != nil {
		if errs.Is(err, io.EOF) {
			return nil, errs.Wrap(err)
		}
		return nil, errs.Wrap(csvdata.ErrInvalidRecord, errs.WithCause(err))
	}
	return nil, errs.Wrap(io.EOF)
}

//Close method is dummy.
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
