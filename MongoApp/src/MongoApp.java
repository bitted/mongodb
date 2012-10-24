import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.DB;
import com.mongodb.WriteConcern;

import java.util.Set;
import java.util.List;

public class MongoApp {
    public static void main(String[] args) throws Exception {

        // connect to the local database server
        Mongo m = new Mongo();

        //m.setWriteConcern(WriteConcern.SAFE);

        DB db = m.getDB( "mongo-app" );

        // get a list of the collections in this database and print them out
        Set<String> colls = db.getCollectionNames();
        for (String s : colls) {
            System.out.println(s);
        }

        // get a collection object to work with
        DBCollection coll = db.getCollection("tickets");

        // make a document and insert it
        BasicDBObject query = new BasicDBObject();
        BasicDBObject update = new BasicDBObject();
        BasicDBObject updatedValues = new BasicDBObject();

        query.put("status", "available");
        updatedValues.put("status", "sold");
        updatedValues.put("user_id", "12345");
        update.put("$set", updatedValues);

        //coll.insert(opts);
        DBObject result = coll.findAndModify(query, updatedValues);
        System.out.println("Updated ticket: " + result);

        //  lets get all the documents in the collection and print them out
        System.out.println("All data:");
        DBCursor cursor = coll.find();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // See if the last operation had an error
        //System.out.println("Last error : " + db.getLastError());

        // see if any previous operation had an error
        //System.out.println("Previous error : " + db.getPreviousError());

        // force an error
        //db.forceError();

        // See if the last operation had an error
        //System.out.println("Last error : " + db.getLastError());

        //db.resetError();

        // release resources
        m.close();
    }

}
