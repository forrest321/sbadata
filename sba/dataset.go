package dataset

import (
	"errors"
	"github.com/buger/jsonparser"
	"github.com/go-resty/resty/v2"
	"github.com/jinzhu/gorm"
	"strings"
)

type DataSet struct {
	gorm.Model
	Type             string `db:"type"`
	Title            string `db:"title"`
	Description      string `db:"desc"`
	Modified         string `db:"last_modified"`
	AccessLevel      string `db:"access_level"`
	Identifier       string `db:"identifier"`
	LandingPage      string `db:"landing_page"`
	License          string `db:"license"`
	PublisherName    string `db:"pub_name"`
	PublisherType    string `db:"pub_type"`
	ContactPointType string `db:"contact_type"`
	ContactFn        string `db:"contact_fn"`
	ContactEmail     string `db:"contact_email"`
	DistType         string `db:"dist_type"`
	DistAccessUrl    string `db:"dist_access_url"`
	DistTitle        string `db:"dist_title"`
	DistMediaType    string `db:"dist_media_type"`
	DistDownloadUrl  string `db:"dist_download_url"`
	Keyword          string `db:"keywords"`
	BureauCode       string `db:"bureau_code"`
	ProgramCode      string `db:"program_code"`
}

type DataSets []DataSet

func GetDataSets(page, limit int) ([]DataSet, error) {
	db, err := gorm.Open("sqlite3", "sba.db")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	ds := &DataSets{}

	offset := (page - 1) * limit

	db.Offset(offset).Limit(limit).Find(&ds)
	return *ds, nil
}

func ImportDataSets() ([]DataSet, error) {
	payload, err := getJsonPayload()
	if err != nil {
		return nil, err
	}

	dataSets, err := parsePayload(payload)
	if err != nil {
		return nil, err
	}

	return dataSets, nil
}

func getJsonPayload() ([]byte, error) {
	var payloadUrl = "https://www.sba.gov/sites/default/files/data.json"
	client := resty.New()
	client.SetAllowGetMethodPayload(true)

	resp, err := client.R().Get(payloadUrl)
	if err != nil {
		return nil, err
	}

	if resp == nil || resp.Body() == nil {
		return nil, errors.New("nil response")
	}

	return resp.Body(), nil
}

func parsePayload(data []byte) ([]DataSet, error) {
	paths := [][]string{
		[]string{"@type"},
		[]string{"title"},
		[]string{"description"},
		[]string{"identifier"},
		[]string{"license"},
		[]string{"publisher", "@type"},
		[]string{"publisher", "name"},
		[]string{"contactPoint", "@type"},
		[]string{"contactPoint", "fn"},
		[]string{"contactPoint", "hasEmail"},
		[]string{"keyword"},
		[]string{"bureauCode"},
		[]string{"programCode"},
	}
	var dataSets []DataSet
	_, outerError := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		ds := &DataSet{}
		jsonparser.EachKey(value, func(idx int, val []byte, vt jsonparser.ValueType, err2 error) {
			v, er := jsonparser.ParseString(val)
			if er != nil {
				return
			}
			switch idx {
			case 0:
				ds.Type = v
			case 1:
				ds.Title = v
			case 2:
				ds.Description = v
			case 3:
				ds.Identifier = v
			case 4:
				ds.License = v
			case 5:
				ds.PublisherType = v
			case 6:
				ds.PublisherName = v
			case 7:
				ds.ContactPointType = v
			case 8:
				ds.ContactFn = v
			case 9:
				ds.ContactEmail = v
			case 10:
				k := strings.ReplaceAll(v, "[", "")
				k = strings.ReplaceAll(k, "]", "")
				k = strings.ReplaceAll(k, "\"", "")
				ds.Keyword = k
			case 11:
				ds.BureauCode = v
			case 12:
				ds.ProgramCode = v
			}
		}, paths...)
		dataSets = append(dataSets, *ds)
	}, "dataset")
	return dataSets, outerError
}
