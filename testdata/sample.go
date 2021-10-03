//go:build run
// +build run

package main

import (
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spiegel-im-spiegel/csvdata"
)

//go:embed sample.csv
var planets string

func main() {
	rc := csvdata.NewRows(csvdata.New(strings.NewReader(planets)), true)
	defer rc.Close() //dummy
	for {
		if err := rc.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintln(os.Stderr, err)
			return
		}
		order, err := rc.ColumnInt64("order", 10)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		fmt.Println("    Order =", order)
		fmt.Println("     Name =", rc.Column("name"))
		mass, err := rc.ColumnFloat64("mass")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		fmt.Println("     Mass =", mass)
		habitable, err := rc.ColumnBool("habitable")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		fmt.Println("Habitable =", habitable)
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
