package dataset

import (
	"errors"
	"fmt"

	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/jinzhu/gorm"
)

type Publisher struct {
	Type string `json:"@type"`
	Name string `json:"name"`
}
type Contact struct {
	Type  string `json:"@type"`
	Name  string `json:"fn"`
	Email string `json:"hasEmail"`
}
type Distribution struct {
	Type        string `json:"@type"`
	AccessURL   string `json:"accessURL"`
	Title       string `json:"title"`
	DownloadURL string `json:"downloadURL"`
}

type DataSet struct {
	gorm.Model
	Type             string         `db:"type" json:"@type"`
	Title            string         `db:"title" json:"title"`
	Description      string         `db:"desc" json:"description"`
	Modified         string         `db:"last_modified" json:"modified"`
	AccessLevel      string         `db:"access_level" json:"accessLevel"`
	Identifier       string         `db:"identifier" json:"identifier"`
	LandingPage      string         `db:"landing_page" json:"landingPage"`
	License          string         `db:"license" json:"license"`
	Publisher        Publisher      `json:"publisher" gorm:"-"`
	PublisherName    string         `db:"pub_name" json:"publisher.name"`
	PublisherType    string         `db:"pub_type" json:"publisher.type"`
	ContactPoint     Contact        `json:"contactPoint" gorm:"-"`
	ContactPointType string         `db:"contact_type" json:"contactPoint.type"`
	ContactFn        string         `db:"contact_fn" json:"contactPoint.fn"`
	ContactEmail     string         `db:"contact_email" json:"hasEmail"`
	Distributions    []Distribution `json:"distribution" gorm:"-"`
	DistType         string         `db:"dist_type" json:"distribution.@type"`
	DistAccessUrl    string         `db:"dist_access_url" json:"distribution.accessURL"`
	DistTitle        string         `db:"dist_title" json:"distribution.title"`
	DistMediaType    string         `db:"dist_media_type" json:"distribution.mediaType"`
	DistDownloadUrl  string         `db:"dist_download_url" json:"distribution.downloadURL"`
	Keyword          string         `db:"keywords" json:"keyword"`
	BureauCode       string         `db:"bureau_code" json:"bureauCode"`
	ProgramCode      string         `db:"program_code" json:"programCode"`
}

type DataSets []DataSet

//easyjson:json
type JsonResponse struct {
	ConformsTo  string `json:"conformsTo"`
	DescribedBy string `json:"describedBy"`
	Context     string `json:"@context"`
	Type        string `json:"@type"`
	Dataset     []struct {
		Type        string `json:"@type"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Modified    string `json:"modified"`
		AccessLevel string `json:"accessLevel"`
		Identifier  string `json:"identifier"`
		LandingPage string `json:"landingPage,omitempty"`
		License     string `json:"license"`
		Publisher   struct {
			Type string `json:"@type"`
			Name string `json:"name"`
		} `json:"publisher"`
		ContactPoint struct {
			Type     string `json:"@type"`
			Fn       string `json:"fn"`
			HasEmail string `json:"hasEmail"`
		} `json:"contactPoint"`
		Distribution []struct {
			Type        string `json:"@type"`
			AccessURL   string `json:"accessURL,omitempty"`
			Title       string `json:"title"`
			MediaType   string `json:"mediaType,omitempty"`
			DownloadURL string `json:"downloadURL,omitempty"`
		} `json:"distribution,omitempty"`
		Keyword            []string `json:"keyword"`
		BureauCode         []string `json:"bureauCode"`
		ProgramCode        []string `json:"programCode"`
		Rights             string   `json:"rights,omitempty"`
		DescribedBy        string   `json:"describedBy,omitempty"`
		Theme              []string `json:"theme,omitempty"`
		AccrualPeriodicity string   `json:"accrualPeriodicity,omitempty"`
		DataQuality        bool     `json:"dataQuality,omitempty"`
		Issued             string   `json:"issued,omitempty"`
		IsPartOf           string   `json:"isPartOf,omitempty"`
		Language           []string `json:"language,omitempty"`
	} `json:"dataset"`
}

func GetDataSet(id string) (*DataSet, error) {
	db, err := gorm.Open("sqlite3", "sba.db")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	ds := &DataSet{}
	db.First(&ds, "identifier = ?", id)
	return ds, nil
}

func GetDataSets(page, limit int) ([]DataSet, error) {
	db, err := gorm.Open("sqlite3", "sba.db")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	ds := &DataSets{}

	if page == 0 && limit == 0 {
		db.Find(&ds)
	} else {
		offset := (page - 1) * limit
		db.Offset(offset).Limit(limit).Find(&ds)
	}

	return *ds, nil
}

func ImportDataSets() ([]DataSet, error) {
	payload, err := getJsonPayload()
	if err != nil {
		fmt.Printf("\nError in getJsonPayload, \n%v\n", err)
		return nil, err
	}

	dataSets, err := parsePayload(payload)
	if err != nil {
		fmt.Printf("\nError in parsePayload, \n%v\n", err)
		return nil, err
	}

	return dataSets, nil
}

func getJsonPayload() ([]byte, error) {
	var payloadUrl = "https://www.sba.gov/data.json"
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
	var dataSets []DataSet
	jr := new(JsonResponse)
	err := jr.UnmarshalJSON(data)
	if err != nil {
		fmt.Printf("\nError in parsePayload, \n%v\n", err)
		return nil, err
	}

	for _, d := range jr.Dataset {
		ds := &DataSet{
			Type:             d.Type,
			Title:            d.Title,
			Description:      d.Description,
			Modified:         d.Modified,
			AccessLevel:      d.AccessLevel,
			Identifier:       d.Identifier,
			LandingPage:      d.LandingPage,
			License:          d.License,
			PublisherName:    d.Publisher.Name,
			PublisherType:    d.Publisher.Type,
			ContactPointType: d.ContactPoint.Type,
			ContactFn:        d.ContactPoint.Fn,
			ContactEmail:     d.ContactPoint.HasEmail,
			Keyword:          strings.Join(d.Keyword, ", "),
			BureauCode:       strings.Join(d.BureauCode, ", "),
			ProgramCode:      strings.Join(d.ProgramCode, ", "),
		}

		if len(d.Distribution) > 0 {
			ds.DistType = d.Distribution[0].Type
			ds.DistAccessUrl = d.Distribution[0].AccessURL
			ds.DistTitle = d.Distribution[0].Title
			ds.DistMediaType = d.Distribution[0].MediaType
			ds.DistDownloadUrl = d.Distribution[0].DownloadURL
		}

		dataSets = append(dataSets, *ds)
	}
	return dataSets, nil
}
