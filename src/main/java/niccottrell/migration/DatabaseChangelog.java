package niccottrell.migration;

import com.github.mongobee.changeset.ChangeLog;
import com.github.mongobee.changeset.ChangeSet;
import com.mongodb.Block;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Build up certain settings
 */
@ChangeLog
public class DatabaseChangelog {

    Logger log = LoggerFactory.getLogger(DatabaseChangelog.class);

    public static final String DB_TEMPLATE = "projTemplate";

    @ChangeSet(order = "001", id = "someChangeId", author = "testAuthor")
    public void importantWorkToDo(MongoDatabase db) {
        // create default objects
        MongoCollection<Document> languages = db.getCollection("languages");
        languages.insertOne(new Document("_id", "de").append("nameNative", "Deutsch"));
        languages.insertOne(new Document("_id", "en").append("nameNative", "English"));
        languages.insertOne(new Document("_id", "es").append("nameNative", "español"));
        languages.createIndex(new Document("nameNative", 1));
    }

    @ChangeSet(order = "002", id = "someChangeId2", author = "testAuthor")
    public void importantWorkToDo2(MongoDatabase db) {
        // remove spanish
        MongoCollection<Document> languages = db.getCollection("languages");
        languages.deleteMany(new Document("_id", "es"));
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
        MongoClient mongoClient = MongoClients.create(MainPlain.MONGO_URI);
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
