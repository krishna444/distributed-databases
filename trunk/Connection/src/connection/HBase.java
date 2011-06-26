package connection;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author krishna
 */
public class HBase {

    HTable table;

    public HBase() {
        try {
            this.table = new HTable(GlobalObjects.Hbase.tableName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public boolean insertSensorData(String row, GlobalObjects.SensorData sensor) throws IOException {
        try {
            Put put = new Put(Bytes.toBytes(row));
            put.add(Bytes.toBytes(GlobalObjects.Hbase.columnFamily),
                    Bytes.toBytes(GlobalObjects.Hbase.dateColumn), Bytes.toBytes(sensor.date));
            put.add(Bytes.toBytes(GlobalObjects.Hbase.columnFamily),
                    Bytes.toBytes(GlobalObjects.Hbase.temperatureColumn), Bytes.toBytes(sensor.temperature));
            put.add(Bytes.toBytes(GlobalObjects.Hbase.columnFamily),
                    Bytes.toBytes(GlobalObjects.Hbase.pressureColumn), Bytes.toBytes(sensor.pressure));
            table.put(put);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return true;
    }
}
