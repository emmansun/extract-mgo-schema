package main

import (
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	cli "gopkg.in/urfave/cli.v1"
)

const (
	CSVFormat  = "csv"
	JSONFormat = "json"

	MaxTryRecords = 100
	MaxGoRoutines = 4
)

type commandInfo struct {
	url    string
	output string
	format string
	dbName string
}

type docField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type docSchema []docField

// Len is the number of elements in the collection.
func (schema docSchema) Len() int {
	return len(schema)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (schema docSchema) Less(i, j int) bool {
	return strings.Compare(schema[i].Name, schema[j].Name) < 0
}

// Swap swaps the elements with indexes i and j.
func (schema docSchema) Swap(i, j int) {
	temp := schema[i]
	schema[i] = schema[j]
	schema[j] = temp
}

var (
	datatabseFlag = cli.StringFlag{
		Name:  "database",
		Usage: "Database connection string. Example: \"mongodb://localhost:3001/meteor\"",
	}
	outputFlag = cli.StringFlag{
		Name:  "output",
		Usage: "Output file",
	}
	formatFlag = cli.StringFlag{
		Name:  "format",
		Usage: "Output file format. Can be \"json\" or \"csv\". Default is \"json\"",
		Value: JSONFormat,
	}
)

var tasks chan string

func addIfNotExists(schema *docSchema, field *docField, fieldSet map[string]struct{}) {
	if _, ok := fieldSet[field.Name]; !ok {
		fieldSet[field.Name] = struct{}{}
		*schema = append(*schema, *field)
	}
}

func getSchema(prefix string, object interface{}, schema *docSchema, fieldSet map[string]struct{}) {
	if object == nil {
		return
	}
	field := new(docField)
	if prefix != "" {
		field.Name = prefix
	}
	switch object.(type) {
	case int:
	case int8:
	case int16:
	case int32:
	case int64:
	case uint:
	case uint8:
	case uint16:
	case uint32:
	case uint64:
		field.Type = "INTEGER"
		addIfNotExists(schema, field, fieldSet)
		break
	case float32:
	case float64:
		field.Type = "DECIMAL"
		addIfNotExists(schema, field, fieldSet)
		break
	case string:
		field.Type = "STRING"
		addIfNotExists(schema, field, fieldSet)
		break
	case bool:
		field.Type = "BOOL"
		addIfNotExists(schema, field, fieldSet)
		break
	case time.Time:
		field.Type = "TIME"
		addIfNotExists(schema, field, fieldSet)
		break
	case bson.ObjectId:
		field.Type = "OBJECTID"
		addIfNotExists(schema, field, fieldSet)
		break
	case bson.Binary:
	case []uint8:
		field.Type = "BINARY"
		addIfNotExists(schema, field, fieldSet)
	case bson.D:
		getStructureSchema(field.Name, object.(bson.D), schema, fieldSet)
		break
	case []interface{}:
		field.Type = "ARRAY"
		addIfNotExists(schema, field, fieldSet)
		for i, v := range object.([]interface{}) {
			if i < MaxTryRecords {
				getSchema(field.Name+"[]", v, schema, fieldSet)
			} else {
				break
			}
		}
		break
	default:
		field.Type = "UNKNOWN"
		addIfNotExists(schema, field, fieldSet)
		log.Printf("%v, Unknown=%v\n", field.Name, reflect.TypeOf(object))
		break
	}
}

func getStructureSchema(prefix string, object bson.D, schema *docSchema, fieldSet map[string]struct{}) {
	for _, v := range object {
		if v.Value == nil {
			continue
		}
		name := prefix
		if prefix == "" {
			name = v.Name
		} else {
			name = prefix + "." + v.Name
		}
		getSchema(name, v.Value, schema, fieldSet)
	}
}

func genCollectionSchema(dbSchema map[string]docSchema, c *mgo.Collection) {
	fieldSet := make(map[string]struct{})
	var results []bson.D
	err := c.Find(bson.M{}).Limit(MaxTryRecords).Sort("-_id").All(&results)
	if err != nil && err == mgo.ErrNotFound {
		dbSchema[c.Name] = docSchema{}
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	var colSchema = docSchema{}
	for _, result := range results {
		getStructureSchema("", result, &colSchema, fieldSet)
	}
	if len(colSchema) > 1 {
		sort.Sort(colSchema[1:])
	}
	dbSchema[c.Name] = colSchema
}

func getDbSchema(db *mgo.Database) map[string]docSchema {
	log.Printf("Extract schema for database %v\n", db.Name)
	defer func(start time.Time) {
		log.Printf("Extract schema for database %v done, used time %v\n", db.Name, time.Now().Sub(start))
	}(time.Now())
	dbSchemas := make(map[string]docSchema)
	collectionNames, err := db.CollectionNames()
	if err != nil {
		log.Fatal(err)
	}
	if len(collectionNames) > 0 {
		var done sync.WaitGroup
		tasks = make(chan string, len(collectionNames))
		for _, collectionName := range collectionNames {
			tasks <- collectionName
		}
		close(tasks)
		routines := MaxGoRoutines
		if routines > len(collectionNames) {
			routines = len(collectionNames)
		}
		for i := 1; i <= MaxGoRoutines; i++ {
			done.Add(1)
			go func(i int) {
				for {
					collectionName, ok := <-tasks
					if !ok {
						done.Done()
						return
					}
					startTime := time.Now()
					genCollectionSchema(dbSchemas, db.C(collectionName))
					log.Printf("Go Routine %v, Extract schema for collection %v, used time %v.\n", i, collectionName, time.Now().Sub(startTime))
				}
			}(i)
		}
		done.Wait()
	}
	return dbSchemas
}

func exportJSON(cmdInfo *commandInfo, schema map[string]docSchema) error {
	schemaJSON, err := json.Marshal(schema)
	if err == nil {
		return ioutil.WriteFile(cmdInfo.output, schemaJSON, 0644)
	}
	return err
}

func exportCSV(cmdInfo *commandInfo, schema map[string]docSchema) error {
	f, err := os.Create(cmdInfo.output)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	for c, fields := range schema {
		if len(fields) > 0 {
			for _, f := range fields {
				err := writer.Write([]string{c, f.Name, f.Type})
				if err != nil {
					return err
				}
			}
		}
	}
	writer.Flush()
	return nil
}

func extractSchema(ctx *cli.Context) error {
	if ctx.NumFlags() == 0 {
		cli.ShowAppHelpAndExit(ctx, -1)
		return nil
	}
	cmdInfo := new(commandInfo)
	if !ctx.GlobalIsSet(datatabseFlag.Name) {
		log.Fatalf("%s is mandatory!", datatabseFlag.Name)
	}
	cmdInfo.url = ctx.GlobalString(datatabseFlag.Name)
	cmdInfo.format = formatFlag.Value
	if ctx.GlobalIsSet(formatFlag.Name) {
		cmdInfo.format = ctx.GlobalString(formatFlag.Name)
	}
	if cmdInfo.format != JSONFormat && cmdInfo.format != CSVFormat {
		cmdInfo.format = JSONFormat
	}
	if !ctx.GlobalIsSet(outputFlag.Name) {
		log.Fatalf("%s is mandatory!", outputFlag.Name)
	}
	cmdInfo.output = ctx.GlobalString(outputFlag.Name)
	dialInfo, err := mgo.ParseURL(cmdInfo.url)
	if err != nil {
		log.Panic(err)
	}

	cmdInfo.dbName = dialInfo.Database
	session, err := mgo.Dial(cmdInfo.url)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	if cmdInfo.dbName == "" {
		log.Fatalf("Please specify database name.\n")
	}
	db := session.DB(cmdInfo.dbName)
	schema := getDbSchema(db)
	if cmdInfo.format == JSONFormat {
		return exportJSON(cmdInfo, schema)
	}
	return exportCSV(cmdInfo, schema)
}

func main() {
	app := cli.NewApp()
	app.Name = "extract mongodb schema"
	app.Description = "extract mongodb schema"
	app.Flags = []cli.Flag{datatabseFlag, outputFlag, formatFlag}
	app.Action = extractSchema
	err := app.Run(os.Args)
	if err != nil {
		log.Panic(err)
	}
}
