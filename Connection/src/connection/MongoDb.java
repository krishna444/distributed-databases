
package connection;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.DB;
import java.net.UnknownHostException;

/**
 *
 * @author Krishna
 */
public class MongoDb {

    //this paramter requires for database operations
    public DBCollection collection=null;

    /*
     * Constructor
     */
    public MongoDb() {
        this.loadCollection();
    }

    /*
     * Loads the collection of mongodb
     */
    private void loadCollection(){
        Mongo m=null;
        try{
            m=new Mongo(GlobalObjects.MongoDb.Server,GlobalObjects.MongoDb.Port);
        }catch(UnknownHostException ex){
            ex.printStackTrace();
        }
        DB db=m.getDB(GlobalObjects.MongoDb.Database);
        this.collection= db.getCollection(GlobalObjects.MongoDb.CollectionName);
    }

    /*
     * Inserts sensor data in the database
     */
    public boolean insertData(GlobalObjects.SensorData sensorData){
        BasicDBObject object=new BasicDBObject();
        object.put(GlobalObjects.MongoDb.DateColumn, sensorData.date);
        object.put(GlobalObjects.MongoDb.TemperatureColumn, sensorData.temperature);
        object.put(GlobalObjects.MongoDb.PressureColumn, sensorData.pressure);
        this.collection.insert(object);
        return true;
    }   

}
