package benchmark;

import com.mongodb.*;

import java.util.Arrays;

public class Producer {
    public static void main(String[] args) throws Exception {
        Mongo m = new Mongo(Arrays.asList(new ServerAddress("localhost", 27017),
                                          new ServerAddress("localhost", 27018),
                                          new ServerAddress("localhost", 27019),
                                          new ServerAddress("localhost", 27020),
                                          new ServerAddress("localhost", 27021)));

        m.setWriteConcern(WriteConcern.REPLICAS_SAFE);

        DB db = m.getDB( "mongo-app" );
        DBCollection queue = db.getCollection("queue");

        while (true)
        {
            Thread.currentThread().sleep(80);
            BasicDBObject newObject = new BasicDBObject();
            newObject.put("status", "new");
            newObject.put("message", "Yeah Couscous!");
            queue.insert(newObject);
        }
    }
}
