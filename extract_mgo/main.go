package main

import (
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	cli "gopkg.in/urfave/cli.v1"
)

const (
	CSVFormat  = "csv"
	JSONFormat = "json"
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

var fieldSet map[string]struct{}

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

func addIfNotExists(schema *docSchema, field *docField) {
	if _, ok := fieldSet[field.Name]; !ok {
		fieldSet[field.Name] = struct{}{}
		*schema = append(*schema, *field)
	}
}

func getStructureSchema(prefix string, object bson.D, schema *docSchema) {
	for _, v := range object {
		if v.Value == nil {
			continue
		}
		field := new(docField)
		if prefix == "" {
			field.Name = v.Name
		} else {
			field.Name = prefix + "." + v.Name
		}
		switch v.Value.(type) {
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
			addIfNotExists(schema, field)
			break
		case float32:
		case float64:
			field.Type = "DECIMAL"
			addIfNotExists(schema, field)
			break
		case string:
			field.Type = "STRING"
			addIfNotExists(schema, field)
			break
		case bool:
			field.Type = "BOOL"
			addIfNotExists(schema, field)
			break
		case time.Time:
			field.Type = "TIME"
			addIfNotExists(schema, field)
			break
		case bson.ObjectId:
			field.Type = "OBJECTID"
			addIfNotExists(schema, field)
			break
		case bson.Binary:
		case []uint8:
			field.Type = "BINARY"
			addIfNotExists(schema, field)
		case bson.D:
			getStructureSchema(field.Name, v.Value.(bson.D), schema)
			break
		case []interface{}:
			field.Type = "ARRAY"
			addIfNotExists(schema, field)
			break
		default:
			field.Type = "UNKNOWN"
			addIfNotExists(schema, field)
			log.Printf("%v, Unknown=%v, %v\n", v.Name, reflect.TypeOf(v.Value), v.Value)
			break
		}
	}
}

func genCollectionSchema(c *mgo.Collection) docSchema {
	fieldSet = make(map[string]struct{})
	var results []bson.D
	err := c.Find(bson.M{}).Limit(100).All(&results)
	if err != nil && err == mgo.ErrNotFound {
		return docSchema{}
	}
	if err != nil {
		log.Fatal(err)
	}
	var colSchema = docSchema{}
	for _, result := range results {
		getStructureSchema("", result, &colSchema)
	}
	return colSchema
}

func getDbSchema(db *mgo.Database) map[string]docSchema {
	dbSchemas := make(map[string]docSchema)
	collectionNames, err := db.CollectionNames()
	if err != nil {
		log.Fatal(err)
	}
	for _, collectionName := range collectionNames {
		dbSchemas[collectionName] = genCollectionSchema(db.C(collectionName))
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
