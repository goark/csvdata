package exceldata_test

import (
	"fmt"

	"github.com/spiegel-im-spiegel/csvdata"
	"github.com/spiegel-im-spiegel/csvdata/exceldata"
	"github.com/xuri/excelize/v2"
)

func ExampleNew() {
	xlsx, err := excelize.OpenFile("testdata/sample.xlsx")
	if err != nil {
		return
	}
	r, err := exceldata.New(xlsx, 0)
	if err != nil {
		return
	}
	rc := csvdata.NewRows(r, true)
	if err := rc.Next(); err != nil {
		return
	}
	fmt.Println(rc.Column("name"))
	// Output:
	// Mercury
}
