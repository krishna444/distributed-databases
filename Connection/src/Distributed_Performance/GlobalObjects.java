
package Distributed_Performance;


/**
 *This class contains the global objects that can be utilised everywhere in the
 * source code
 * @author Dhanpati
 */
public class GlobalObjects {

    public static class SensorData{
        public String date;
        public float temperature;
        public float pressure;
        /*
         * Constructors
         */
        public SensorData(){

        }
        /*
         * Constructors
         */
        public SensorData(String date,float temp,float press){
            this.date=date;
            this.temperature=temp;
            this.pressure=press;
        }
        
    }

    /*
     * Cassandra Properties
     */
    public static class Cassandra{
        public static String KeySpaceName="SensorSpace";
        public static String ColumnFamily="Sensor";
        public static int ReplicationFactor=1;
        public static String DateColumn="date";
        public static String TempColumn="temperature";
        public static String PressureColumn="Pressure";

        public static String Server="localhost";
        public static int Port=9160;
    }

    /*
     * Mongodb properties
     */
    public static class MongoDb{
        public static String Server="localhost";
        public static int Port=27017;
        public static String Database="sensordb";
        public static String CollectionName="sensor";
        public static String DateColumn="DateTime";
        public static String PressureColumn="Pressure";
        public static String TemperatureColumn="Temperature";

    }
    public static class Hypertable{
        public static String Server="localhost";
        public static int Port=38080;
        public static String namespace="weather";
        public static String tableName="sensor";

    }
    public static class Hbase{
        public static String Server="localhost";
        public static String tableName="sensor";
        public static String columnFamily="cf";
        public static String dateColumn="Date";
        public static String temperatureColumn="Temperature";
        public static String pressureColumn="Pressure";
    }

    public enum DatabaseType{
        CASSANDRA,
        MONGODB,
        HYPERTABLE,
        HBASE
    }
}
