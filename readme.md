# Mongobee examples

This is intended to show some working examples of [mongoBee](https://github.com/mongobee/mongobee)

Note that indexes are built in the background to avoid locking the database in production.

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
