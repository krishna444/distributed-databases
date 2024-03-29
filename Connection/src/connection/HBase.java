package connection;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Observable;
import java.util.Iterator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author krishna
 */
public class HBase extends Observable implements Runnable {

    HTable table;
    private long executionTime;
    private List<String[]> sensorList;
    private int dumpFileSize;
    private String fileName;
    ResultExporter resultExporter;

    public HBase() {
        this.fileName = "result/hbase";
        this.resultExporter = new ResultExporter();
        try {
            this.table = new HTable(GlobalObjects.Hbase.tableName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void insert(List<String[]> sensorList, int fileSize) {
        this.sensorList = sensorList;
        this.dumpFileSize = fileSize;
        Thread insertThread = new Thread(this, "Insert Data in HBase");
        insertThread.start();
    }

    public void run() {
        this.sendStatusMessage("Insertion Started(HBase)...");
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
                this.insertSensorData("row" + rowCount, sensorData);
            } catch (Exception ex) {
            }

            if (rowCount % progressInterval == 0) {
                int completed = rowCount / progressInterval;
                this.sendStatusMessage(completed + "% completed(Hbase).");
            }
            if (rowCount % sizeInterval == 0) {
                this.resultExporter.writeInsertionInfo(rowCount / sizeInterval, this.executionTime / 1000);
            }

            rowCount++;
        }
        this.sendStatusMessage("Successsfully Finised(HBase)");
        this.sendStatusMessage("Time Taken:" + this.executionTime + " Milliseconds.");
        try {
            this.resultExporter.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public boolean insertSensorData(String row, GlobalObjects.SensorData sensor) throws IOException {
        long startTime = Calendar.getInstance().getTimeInMillis();
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
        this.executionTime += Calendar.getInstance().getTimeInMillis() - startTime;
        return true;
    }

    /**
     * Fetches the data
     * @param fetchLimit Limit of fetching data
     * @return time required to fetch
     * @throws IOException
     */
    public long fetchData(int fetchLimit) throws IOException {
        long fetchTime = 0;
        long startTime = Calendar.getInstance().getTimeInMillis();
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes(GlobalObjects.Hbase.columnFamily), Bytes.toBytes(GlobalObjects.Hbase.temperatureColumn));
        ResultScanner scanner = this.table.getScanner(s);
        int count = 0;
        for (Result rr : scanner) {
            System.out.println("Row Found:" + rr);
            if (count++ > fetchLimit) {
                break;
            }
        }
        fetchTime = Calendar.getInstance().getTimeInMillis() - startTime;
        System.out.println("Execution Time="+fetchTime+"milliseconds.");
        return fetchTime;
    }

    private void sendStatusMessage(String message) {
        this.setChanged();
        this.notifyObservers(message);
    }
}
