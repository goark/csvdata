package csvdata

import "errors"

var (
	ErrNullPointer      = errors.New("null reference instance")
	ErrNullValue        = errors.New("null value")
	ErrInvalidRecord    = errors.New("invalid record")
	ErrOutOfIndex       = errors.New("out of index")
	ErrInvalidSheetName = errors.New("invalid sheet name in Excel data")
	ErrInvalidExcelData = errors.New("invalid Excel data")
)

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
