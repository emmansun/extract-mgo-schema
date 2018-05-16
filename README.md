# extract-mgo-schema
This is a golang tool to extract mongodb schema. The main steps are:

1. List all collections from mongdodb database
2. Handle collection one by one
	1. Select 100 documents and analysis document's fields type according object real type. Using []bson.D as result type: `	var results []bson.D 
	err := c.Find(bson.M{}).Limit(MaxTryRecords).Sort("-_id").All(&results)`
	1. For slice []interface{}, also handle at most 100 records.
	1. Handle bson.D as embedded document.

**Sample command**: extract_mgo.exe -database mongodb://db_owner:db_owner@localhost:47017/sampledb -format csv -output mongo_schema.csv
