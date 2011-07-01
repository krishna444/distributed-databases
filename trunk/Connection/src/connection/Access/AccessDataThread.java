package connection.Access;

import connection.CassandraConnector;
import connection.GlobalObjects.DatabaseType;
import connection.HBase;
import connection.Hypertable;
import connection.MongoDb;

/**
 * This accesses the data from the database.
 * @author krishna
 */
public class AccessDataThread extends Thread {

    private DatabaseType databaseType;
    private int fetchTime = 0;
    //Databases
    private CassandraConnector cassandra;
    private MongoDb mongoDb;
    private HBase hbase;
    private Hypertable hyperTable;
    //Data limit to fetch
    private int fetchLimit=0;
    /**
     * Constructor
     * @param databaseType type of database
     * @param limit limit of data
     */
    public AccessDataThread(DatabaseType databaseType,int limit) {
        this.databaseType = databaseType;
        this.fetchLimit=limit;
        this.initialiseDatabases();
    }

    @Override
    public void run() {
        switch (this.databaseType) {
            case CASSANDRA:
                this.cassandra.fetchData(this.fetchLimit);
                break;
            case MONGODB:
                break;
            case HBASE:
                break;
            case HYPERTABLE:
                break;
            default:
                break;
        }
    }

    private DatabaseType getDatabaseType() {
        return this.databaseType;
    }

    private void initialiseDatabases() {
        this.cassandra = new CassandraConnector();
        this.mongoDb = new MongoDb();
        this.hyperTable = new Hypertable();
        this.hbase = new HBase();
    }
}
