package csvdata_test

import (
	"fmt"

	"github.com/goark/csvdata"
)

func ExampleNew() {
	file, err := csvdata.OpenFile("testdata/sample.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	rc := csvdata.NewRows(csvdata.New(file), true)
	defer rc.Close()

	if err := rc.Next(); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(rc.Column("name"))
	// Output:
	// Mercury
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
