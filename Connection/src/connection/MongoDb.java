package connection;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;

/**
 *
 * @author Krishna
 */
public class MongoDb extends Observable implements Runnable {

    //this paramter requires for database operations
    //public DBCollection collection = null;
    private long executionTime;
    private List<String[]> sensorList;
    private int dumpFileSize;
    private String fileName;
    ResultExporter resultExporter;
    //DBCollection collection;
    DB db;

    /*
     * Constructor
     */
    public MongoDb() {
        this.fileName = "result/mongodb";
        this.resultExporter = new ResultExporter();
        this.loadDB();
    }

    /*
     * Loads the collection of mongodb
     */
    private DBCollection getCollection() {
        this.loadDB();
        return this.db.getCollection(GlobalObjects.MongoDb.CollectionName);
    }

    private void loadDB(){
        Mongo m = null;
        try {
            m = new Mongo(GlobalObjects.MongoDb.Server, GlobalObjects.MongoDb.Port);
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        }
        this.db=m.getDB(GlobalObjects.MongoDb.Database);
    }

    public void insert(List<String[]> sensorList, int fileSize) {
        this.sensorList = sensorList;
        this.dumpFileSize = fileSize;
        Thread insertThread = new Thread(this, "Insert Data in MongoDb");
        insertThread.start();
    }

    public void run() {
        this.sendStatusMessage("Insertion Started(MongoDb)...");
        int progressInterval = this.sensorList.size() / 100;
        int sizeInterval = this.sensorList.size() / this.dumpFileSize;
        try {
            this.resultExporter.open(this.fileName + "." + this.dumpFileSize);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        this.executionTime = 0;
        int rowCount = 0;
        Iterator<String[]> iterator = this.sensorList.iterator();
        while (iterator.hasNext()) {
            String[] value = iterator.next();
            GlobalObjects.SensorData sensorData = new GlobalObjects.SensorData();
            sensorData.date = value[0];
            sensorData.temperature = Float.parseFloat(value[1]);
            sensorData.pressure = Float.parseFloat(value[2]);
            try {
                this.insertData(sensorData);
            } catch (Exception ex) {
            }

            if (rowCount % progressInterval == 0) {
                int completed = rowCount / progressInterval;
                this.sendStatusMessage(completed + "% completed(MongoDb).");
            }
            if (rowCount % sizeInterval == 0) {
                this.resultExporter.writeInsertionInfo(rowCount / sizeInterval, this.executionTime / 1000);
            }

            rowCount++;
        }
        this.sendStatusMessage("Successsfully Finised(MongoDb)");
        this.sendStatusMessage("Time Taken:" + this.executionTime + " Milliseconds.");
        try {
            this.resultExporter.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }


    /*
     * Inserts sensor data in the database
     */
    public boolean insertData(GlobalObjects.SensorData sensorData) {
        long startTime = Calendar.getInstance().getTimeInMillis();
        BasicDBObject object = new BasicDBObject();
        object.put(GlobalObjects.MongoDb.DateColumn, sensorData.date);
        object.put(GlobalObjects.MongoDb.TemperatureColumn, sensorData.temperature);
        object.put(GlobalObjects.MongoDb.PressureColumn, sensorData.pressure);
        this.db.getCollection(GlobalObjects.MongoDb.CollectionName).insert(object);
        this.executionTime += Calendar.getInstance().getTimeInMillis() - startTime;
        return true;
    }

    public long fetchData(int fetchLimit) {
        long fetchTime = Calendar.getInstance().getTimeInMillis();
        DBCursor cursor = this.db.getCollection(GlobalObjects.MongoDb.CollectionName).find();
        int count = 0;
        while (cursor.hasNext() && count++ < fetchLimit) {
            String date = cursor.next().get(GlobalObjects.MongoDb.DateColumn).toString();
            String pressure = cursor.next().get(GlobalObjects.MongoDb.TemperatureColumn).toString();
            String temperature = cursor.next().get(GlobalObjects.MongoDb.PressureColumn).toString();
            System.out.println("Date=" + date + " Press=" + pressure + " Temp=" + temperature);
        }
        fetchTime = Calendar.getInstance().getTimeInMillis() - fetchTime;
        System.out.println("Execution Time:" + fetchTime + "milliseconds.");
        return fetchTime;
    }

    private void sendStatusMessage(String message) {
        this.setChanged();
        this.notifyObservers(message);
    }
}
