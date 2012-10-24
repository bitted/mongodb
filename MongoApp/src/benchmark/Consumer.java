package benchmark;

import com.mongodb.*;

import java.util.Arrays;
import java.util.Date;

public class Consumer {
    public static void main(String[] args) throws Exception {
        Mongo m = new Mongo(Arrays.asList(new ServerAddress("localhost", 27017),
                                          new ServerAddress("localhost", 27018),
                                          new ServerAddress("localhost", 27019),
                                          new ServerAddress("localhost", 27020),
                                          new ServerAddress("localhost", 27021)));

        m.setWriteConcern(WriteConcern.REPLICAS_SAFE);

        DB db = m.getDB( "mongo-app" );
        DBCollection queue = db.getCollection("queue");

        BasicDBObject statusUpdate = new BasicDBObject();
        BasicDBObject newStatus = new BasicDBObject();
        BasicDBObject processingStatus = new BasicDBObject();
        BasicDBObject doneStatus = new BasicDBObject();
        BasicDBObject timeoutStatus = new BasicDBObject();
        BasicDBObject id = new BasicDBObject();

        newStatus.put("status", "new");
        processingStatus.put("status", "processing");
        doneStatus.put("status", "done");
        timeoutStatus.put("status", "timeout");

        DBObject message = null;

        while (true)
        {
            try
            {
                // Try to get a message which is "timeout" if exists

                message = queue.findAndModify(timeoutStatus, processingStatus);

                if (message == null)
                {
                    // Get a message and flag it as "processing"
                    statusUpdate.put("$set", processingStatus);
                    message = queue.findAndModify(newStatus, statusUpdate);
                }

                if (message != null)
                {
                    // Processing
                    id.put("_id", message.get("_id"));
                    System.out.println(new Date().toString() + " " + message.toString());
                    Thread.currentThread().sleep(12);

                    if (Math.random() < 0.1)
                    {
                        // To mock random fail in processing queue
                        statusUpdate.put("$set", timeoutStatus);
                        queue.findAndModify(id, statusUpdate);
                        Thread.currentThread().sleep(1000);
                        continue;
                    }

                    // end of processing, update the message as "done"
                    id.put("_id", message.get("_id"));
                    statusUpdate.put("$set", doneStatus);
                    queue.findAndModify(id, statusUpdate);
                }
            }
            catch (Exception ex)
            {
                System.out.println("Got exception!");

            }
        }
    }
}
