# Mongobee examples

This is intended to show some working examples of [mongoBee](https://github.com/mongobee/mongobee) - a Java tool which helps you to manage changes in your MongoDB and synchronize them with your application. The concept is very similar to other db migration tools such as Liquibase or Flyway but without using XML/JSON/YML files.

Note that indexes are built in the background to avoid locking the database in production.

## Changeset
 
The example ChangeLog shows:
 * Creating different types of indexes
 * Creating 'core' documents from code
 * Cloning 'core' collections from a template database (not version controlled)
 * Importing documents from JSON content under version control
 * Migrating to a new schema by adding new fields or transforming existing fields to a new data type

### addDefaultField

Changes schema design with new default field. It uses two calls of updateMany to update documents in place on the server with minimal data transfer 

Note: runAlways = true since the filter itself excludes any documents that have already been migrated
     
### changeFieldType
     
Migrates the points field to a decimal version (with a different field name) as per [best practices](https://docs.mongodb.com/manual/tutorial/model-monetary-data/).
         
Note: Order is 402 to guarantee it is run after addDefaultField above

# Build

Build with `mvn package -DskipTests` (or just `mvn package` if you have a mongod running on localhost)

# Run

Then run with `java -jar target/mongobee-demo-1.0-jar-with-dependencies.jar --uri <URI> --db <DB>`
 
## Usage

```
usage: 
 -b,--foreground       Build indexes in the foreground (Default: false = background)
 -c,--uri <arg>        MongoDB connection details (default 'mongodb://localhost:27017' )
 -d,--database <arg>   MongoDB database to update
    --h                Print help only```
