# [csvdata] -- Reading CSV Data

[![check vulns](https://github.com/goark/csvdata/workflows/vulns/badge.svg)](https://github.com/goark/csvdata/actions)
[![lint status](https://github.com/goark/csvdata/workflows/lint/badge.svg)](https://github.com/goark/csvdata/actions)
[![GitHub license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://raw.githubusercontent.com/goark/csvdata/master/LICENSE)
[![GitHub release](https://img.shields.io/github/release/goark/csvdata.svg)](https://github.com/goark/csvdata/releases/latest)

This package is required Go 1.16 or later.

**Migrated repository to [github.com/goark/csvdata][csvdata]**

## Import

```go
import "github.com/goark/csvdata"
```

## Usage

```go
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
```

### Reading from Excel file

```go
package exceldata_test

import (
	"fmt"

	"github.com/goark/csvdata"
	"github.com/goark/csvdata/exceldata"
)

func ExampleNew() {
	xlsx, err := exceldata.OpenFile("testdata/sample.xlsx", "")
	if err != nil {
		fmt.Println(err)
		return
	}
	r, err := exceldata.New(xlsx, "")
	if err != nil {
		fmt.Println(err)
		return
	}
	rc := csvdata.NewRows(r, true)
	defer rc.Close() //dummy

	if err := rc.Next(); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(rc.Column("name"))
	// Output:
	// Mercury
}
```

### Reading from LibreOffice Calc file

```go
package calcdata_test

import (
	"fmt"

	"github.com/goark/csvdata"
	"github.com/goark/csvdata/calcdata"
)

func ExampleNew() {
	ods, err := calcdata.OpenFile("testdata/sample.ods")
	if err != nil {
		fmt.Println(err)
		return
	}
	r, err := calcdata.New(ods, "")
	if err != nil {
		fmt.Println(err)
		return
	}
	rc := csvdata.NewRows(r, true)
	defer rc.Close() //dummy

	if err := rc.Next(); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(rc.Column("name"))
	// Output:
	// Mercury
}
```

## Modules Requirement Graph

[![dependency.png](./dependency.png)](./dependency.png)

[csvdata]: https://github.com/goark/csvdata "goark/csvdata: Reading CSV Data"
