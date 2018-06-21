package niccottrell.migration;

import com.github.mongobee.changeset.ChangeLog;
import com.github.mongobee.changeset.ChangeSet;
import com.mongodb.Block;
import com.mongodb.MongoWriteException;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.apache.commons.io.FileUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Build up certain settings - build indexes in the background (by default)
 */
@ChangeLog
public class DatabaseChangelog {

    Logger log = LoggerFactory.getLogger(DatabaseChangelog.class);

    public static final String DB_TEMPLATE = "projTemplate";

    @ChangeSet(order = "001", id = "someChangeId", author = "nic")
    public void importantWorkToDo(MongoDatabase db) {
        // create default objects
        MongoCollection<Document> languages = db.getCollection("languages");
        languages.insertOne(new Document("_id", "de").append("nameNative", "Deutsch"));
        languages.insertOne(new Document("_id", "en").append("nameNative", "English"));
        languages.insertOne(new Document("_id", "es").append("nameNative", "español"));
        languages.createIndex(new Document("nameNative", 1),
                new IndexOptions().background(true));
    }

    @ChangeSet(order = "002", id = "someChangeId2", author = "nic")
    public void importantWorkToDo2(MongoDatabase db) {
        // remove spanish
        MongoCollection<Document> languages = db.getCollection("languages");
        languages.deleteMany(new Document("_id", "es"));
    }

    @ChangeSet(order = "002", id = "otherIndexes", author = "nic2")
    public void otherIndexes(MongoDatabase db) {
        // Create some complex indexes on this collection
        MongoCollection<Document> collection = db.getCollection("complexCollection");
        // Example document
        collection.insertOne(new Document("sparseField", "A")
                .append("field1", 123)
                .append("field2", "abc")
                .append("field3", new Date())
                .append("expireAt", new Date(System.currentTimeMillis() + 24 * 3600 * 1000)));
        // Build a sparse index
        collection.createIndex(new Document("sparseField", 1),
                new IndexOptions()
                        .sparse(true)
                        .background(true));
        // Build a unique, compound index
        collection.createIndex(new Document("field1", 1).append("field2", 1).append("field3", -1),
                new IndexOptions()
                        .unique(true)
                        .background(true));
        // Build an expires index (Note: expireAt is a ISODate field)
        collection.createIndex(new Document("expireAt", 1),
                new IndexOptions()
                        .expireAfter(0L, TimeUnit.SECONDS)
                        .background(true));
    }

    /**
     * Change schema design with new default field.
     * Note: runAlways = true since the filter itself excludes any documents that have already been migrated
     */
    @ChangeSet(order = "401", id = "addDefaultField", author = "nic", runAlways = true)
    public void addDefaultField(MongoDatabase db) {
        // Add new "freqTravelPoints" field and give "Gold" members 100 points, otherwise 0
        // Change from:
        // { name: "John Smith",
        //   status: "Gold"
        //    ... }
        //  to
        // { name: "John Smith",
        //   status: "Gold",
        //   freqTravelPoints: 100
        //    ... }
        MongoCollection<Document> collection = db.getCollection("customers");
        // We want to avoid loading any documents over the network (imagine we have 1M customers!)
        // Update all Gold customers first with 100 points
        collection.updateMany(Filters.and(
                Filters.exists("freqTravelPoints", false),
                Filters.eq("status", "Gold")
        ), Updates.set("freqTravelPoints", 100));
        // Update all other customers with 0 points
        collection.updateMany(Filters.and(
                Filters.exists("freqTravelPoints", false)
        ), Updates.set("freqTravelPoints", 0));
        // Now all customers should have the freqTravelPoints field (except for newly inserted documents)
    }

    /**
     * Migrate the points field to a decimal version (with a different field name)
     * See: https://docs.mongodb.com/manual/tutorial/model-monetary-data/
     * <p>
     * Note: Order is 402 to guarantee it is run after addDefaultField above
     */
    @ChangeSet(order = "402", id = "changeFieldType", author = "nic", runAlways = true)
    public void changeFieldType(MongoDatabase db) {
        // The new "travelCredits" field is 1/10 of the original freqTravelPoints value
        // Change from:
        // { name: "David Smith",
        //   status: "Gold"
        //   freqTravelPoints: 150
        //    ... }
        //  to
        // { name: "David Smith",
        //   status: "Gold",
        //   freqTravelPoints: 150
        //   travelCredits: 15.0
        //    ... }
        MongoCollection<Document> collection = db.getCollection("customers");
        // Unlike in addDefaultField(), we need to load and save documents in the client (i.e. network transfers)
        AggregateIterable<Document> iterable = collection.aggregate(Arrays.asList(
                // Only look for customers who do not already have a travelCredits field
                Aggregates.match(
                        Filters.and(
                                Filters.type("freqTravelPoints", BsonType.INT32),
                                // only if the field does not already exist
                                Filters.exists("travelCredits", false)
                        )
                ),
                // Only consider the freqTravelPoints field
                Aggregates.project(new Document("freqTravelPoints", 1)),
                // Now caclulate the new value directly in MongoDB (we could also do it in code)
                Aggregates.addFields(new Field<>("travelCredits",
                        new Document("$multiply", Arrays.asList("$freqTravelPoints", Decimal128.parse("0.1"))))
                )
        ));
        for (Document calculated : iterable) {
            // we'll use findOneAndUpdate rather than replaceOne because:
            //   1. we avoid sending the entire customer document over the wire (could be large)
            //   2. we only change the new field (simultaneous changes from other threads will not be overwritten)
            collection.findOneAndUpdate(
                    Filters.eq(calculated.get("_id")),
                    Updates.set("travelCredits", calculated.get("travelCredits")));
        }
        // Now all customers should have the travelCredits field as a Decimale
    }

    @ChangeSet(order = "600", id = "loadSchedules", author = "nic")
    public void loadSchedules(MongoDatabase db) {
        // task implementation
        loadCollection(db, "schedules", "schedules.json");
    }

    private void loadCollection(MongoDatabase db, String coll, String filename) {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource(filename).getFile());
            String string = FileUtils.readFileToString(file);
            Document doc = Document.parse(string);
            MongoCollection<Document> collection = db.getCollection(coll);
            List<Document> values = (List<Document>) doc.get("data");
            for (Document value : values) {
                collection.insertOne(value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading file: " + filename, e);
        }
    }

    @ChangeSet(order = "998", id = "cloneStations", author = "nic")
    public void finalStuff1(MongoDatabase db) {
        // task implementation
        cloneCollection(db, "stations");
    }

    @ChangeSet(order = "999", id = "cloneTrains", author = "nic")
    public void finalStuff2(MongoDatabase db) {
        // task implementation
        cloneCollection(db, "trainTypes");
    }

    private void cloneCollection(MongoDatabase dbTarget, String collname) {
        MongoClient mongoClient = MongoClients.create(Main.MONGO_URI);
        MongoDatabase dbTemplate = mongoClient.getDatabase(DB_TEMPLATE);
        MongoCollection<Document> collTemplate = dbTemplate.getCollection(collname);
        MongoCollection<Document> collTarget = dbTarget.getCollection(collname);
        // use streams to copy over documents
        collTemplate.find().forEach((Block<Document>) doc -> {
            try {
                collTarget.insertOne(doc);
            } catch (MongoWriteException e) {
                log.error("Skipping already exists", e);
            }
        });
        log.info("Finished clone of " + collname);
    }

}
