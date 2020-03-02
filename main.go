package main

import (
	"html/template"
	"io"
	"net/http"
	"strconv"

	dataset "skillsTest/sba"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

type Template struct {
	templates *template.Template
}

var t *Template

func init() {
	db, err := gorm.Open("sqlite3", "sba.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.AutoMigrate(&dataset.DataSet{})
	ds, err := dataset.ImportDataSets()

	if err != nil {
		panic(err)
	}

	for i, _ := range ds {
		db.Create(&ds[i])
	}
	t = &Template{
		templates: template.Must(template.ParseGlob("web/templates/*.html")),
	}
}

func main() {
	e := echo.New()
	e.Use(middleware.Logger())
	e.Renderer = t
	e.Static("/", "web/assets")
	e.GET("/", indexHandler)
	e.GET("/datasets", datasetHandler)
	e.Logger.Fatal(e.Start(":1323"))
}

func (t *Template) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

func indexHandler(c echo.Context) error {
	t.templates = template.Must(template.ParseGlob("web/templates/*.html"))
	page, limit := getPageAndLimit(c)
	ds, err := dataset.GetDataSets(page, limit)
	if err != nil {
		c.Logger().Fatal(err)
		return err
	}
	return c.Render(http.StatusOK, "index.html", ds)
}

func datasetHandler(c echo.Context) error {
	page, limit := getPageAndLimit(c)
	ds, err := dataset.GetDataSets(page, limit)
	if err != nil {
		c.Logger().Fatal(err)
		return err
	}
	return c.JSON(http.StatusOK, ds)
}

func getPageAndLimit(c echo.Context) (int, int) {
	page, _ := strconv.Atoi(c.QueryParam("page"))
	limit, _ := strconv.Atoi(c.QueryParam("limit"))

	// Defaults
	if page == 0 {
		page = 1
	}
	if limit == 0 {
		limit = 1000
	}
	return page, limit
}
