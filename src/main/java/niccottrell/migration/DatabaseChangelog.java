package niccottrell.migration;

import com.github.mongobee.changeset.ChangeLog;
import com.github.mongobee.changeset.ChangeSet;
import com.mongodb.Block;
import com.mongodb.MongoWriteException;
import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
