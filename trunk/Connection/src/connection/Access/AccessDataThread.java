package connection.Access;

import connection.CassandraConnector;
import connection.GlobalObjects.DatabaseType;
import connection.HBase;
import connection.Hypertable;
import connection.MongoDb;
import java.io.IOException;
import org.apache.thrift.TException;
import org.hypertable.thriftgen.ClientException;

/**
 * This accesses the data from the database.
 * @author krishna
 */
public class AccessDataThread extends Thread {

    private DatabaseType databaseType;
    private long fetchTime = 0;
    //Databases
    private CassandraConnector cassandra;
    private MongoDb mongoDb;
    private HBase hbase;
    private Hypertable hyperTable;
    //Data limit to fetch
    private int fetchLimit = 0;

    /**
     * Constructor
     * @param databaseType type of database
     * @param limit limit of data
     */
    public AccessDataThread(DatabaseType databaseType, int limit) {
        this.databaseType = databaseType;
        this.fetchLimit = limit;
        this.initialiseDatabases();
    }

    @Override
    public void run() {
        switch (this.databaseType) {
            case CASSANDRA:
                this.fetchTime = this.cassandra.fetchData(this.fetchLimit);
                break;
            case MONGODB:
                this.fetchTime = this.mongoDb.fetchData(this.fetchLimit);
                break;
            case HBASE:
                try {
                    this.fetchTime = this.hbase.fetchData(this.fetchLimit);
                } catch (IOException ex) {
                    //
                }
                break;
            case HYPERTABLE:
                try {
                    this.fetchTime = this.hyperTable.fetchData(this.fetchLimit);
                } catch (ClientException ex) {
                } catch (TException ex) {
                }
                break;
            default:
                this.fetchTime=this.cassandra.fetchData(this.fetchLimit);
                break;
        }
    }

    private DatabaseType getDatabaseType() {
        return this.databaseType;
    }

    private void initialiseDatabases() {
        if(this.databaseType==DatabaseType.CASSANDRA)
            this.cassandra = new CassandraConnector();
        else if(this.databaseType==DatabaseType.MONGODB)
            this.mongoDb = new MongoDb();
        else if(this.databaseType==DatabaseType.HYPERTABLE)
            this.hyperTable = new Hypertable();
        else if(this.databaseType==DatabaseType.HBASE)
            this.hbase = new HBase();
        else
            this.hbase=new CassandraConnector();
    }

    /**
     * Gets fetch time
     * @return fetch time
     */
    public long getFetchTime(){
        return this.fetchTime;
    }
}
