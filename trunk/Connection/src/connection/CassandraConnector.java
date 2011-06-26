package connection;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import javax.swing.JOptionPane;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author root
 */
public class CassandraConnector extends Observable implements Runnable {

    TTransport tr = new TSocket(GlobalObjects.Cassandra.Server, GlobalObjects.Cassandra.Port);
    Cassandra.Client client = null;
    private long executionTime = 0;
    private List<String[]> sensorList;
    private int dumpFileSize = 0;
    private String fileName = "cassandra.csv";

    public CassandraConnector() {
        try {
            this.Connect();
        } catch (TTransportException ex) {
            ex.printStackTrace();
        } catch (TException ex) {
            ex.printStackTrace();
        } catch (InvalidRequestException ex) {
            ex.printStackTrace();
        }
    }

    /*
     * Creates the Client
     */
    public Cassandra.Client Connect() throws TTransportException, TException, InvalidRequestException {
        TFramedTransport tf = new TFramedTransport(tr);
        TProtocol proto = new TBinaryProtocol(tf);
        this.client = new Cassandra.Client(proto);
        tr.open();
        client.set_keyspace(GlobalObjects.Cassandra.KeySpaceName);
        return client;
    }
    /*
     * Closes the connection
     */

    public void Close() {
        this.tr.close();
    }

    public void insert(List<String[]> sensorList, int fileSize) {
        this.sensorList = sensorList;
        this.dumpFileSize = fileSize;
        Thread insertThread = new Thread(this, "Insert Data in Cassandra");
        insertThread.start();
    }

    public void run() {
        this.sendStatusMessage("Starting insertion of sensor data in Cassandra....");
        int progressInterval = sensorList.size() / 100;
        int sizeInterval = sensorList.size() / this.dumpFileSize;


        //CSV Writer
        CSVWriter csvWriter=null;
        try{
            csvWriter=new CSVWriter(new FileWriter(this.fileName), ',');
        }catch(IOException ex){
           ex.printStackTrace();
        }

        Iterator<String[]> iterator = sensorList.iterator();
        int rowCount = 0;
        while (iterator.hasNext()) {
            String[] data = iterator.next();
            GlobalObjects.SensorData sensor = new GlobalObjects.SensorData(data[0], Float.parseFloat(data[1]), Float.parseFloat(data[2]));
            try {
                this.insertSensorData("row" + rowCount, sensor);
            } catch (Exception ex) {
                //
                System.out.println(ex.toString());
            }

            if (rowCount % sizeInterval == 0) {
                int size=(int) rowCount / sizeInterval;
                float time=this.executionTime/1000;
                csvWriter.writeNext(new String[]{Integer.toString(size), Float.toString(time)});
                File f=new File("test.txt");
                try{
                FileWriter writer=new FileWriter(f);
                writer.write("Hello this is test");
                }catch(Exception ex){
                    
                }
            }

            if (rowCount % progressInterval == 0) {
                float completed = (float) (100 * rowCount) / sensorList.size();
                this.sendStatusMessage(completed + "% completed(Cassandra)...");
            }
            rowCount++;
        }
        this.sendStatusMessage("Successsfully Finised(Cassandra)");
        this.sendStatusMessage("Time Taken:" + this.executionTime + " Milliseconds.");

        try {
            csvWriter.close();
        } catch (Exception ex) {
            //
        }
    }

    /*
     * Inserts the sensor data in the database
     */
    public boolean insertSensorData(String rowKey, GlobalObjects.SensorData sensorData)
            throws Exception {
        //Cassandra.Client client=this.getClient();
        //ColumnPath colPath=new ColumnPath(GlobalObjects.ColumnFamily);
        //colPath.setColumn(MyByteBuffer.str_to_bb(GlobalObjects.TempColumn));
        long startTime = Calendar.getInstance().getTimeInMillis();
        ColumnParent parent = new ColumnParent(GlobalObjects.Cassandra.ColumnFamily);
        long timeStamp = System.currentTimeMillis();
        //date
        Column column = new Column();
        column.timestamp = timeStamp;
        column.name = MyByteBuffer.str_to_bb(GlobalObjects.Cassandra.DateColumn);
        column.value = MyByteBuffer.str_to_bb(sensorData.date);
        this.client.insert(MyByteBuffer.str_to_bb(rowKey), parent, column, ConsistencyLevel.ONE);
        //temperature
        column = new Column();
        column.timestamp = timeStamp;
        column.name = MyByteBuffer.str_to_bb(GlobalObjects.Cassandra.TempColumn);
        column.value = MyByteBuffer.str_to_bb(Float.toString(sensorData.temperature));
        this.client.insert(MyByteBuffer.str_to_bb(rowKey), parent, column, ConsistencyLevel.ONE);
        //pressure
        column = new Column();
        column.timestamp = timeStamp;
        column.name = MyByteBuffer.str_to_bb(GlobalObjects.Cassandra.PressureColumn);
        column.value = MyByteBuffer.str_to_bb(Float.toString(sensorData.pressure));
        this.client.insert(MyByteBuffer.str_to_bb(rowKey), parent, column, ConsistencyLevel.ONE);
        this.executionTime += Calendar.getInstance().getTimeInMillis() - startTime;
        return true;
    }


    /*
     * Gets the data  for test purpoes only!
     */
    public String getData(String rowKey) throws Exception {
        //Cassandra.Client client=this.getClient();
        ColumnPath colPath = new ColumnPath(GlobalObjects.Cassandra.ColumnFamily);
        colPath.setColumn(MyByteBuffer.str_to_bb(GlobalObjects.Cassandra.TempColumn));
        Column col = this.client.get(MyByteBuffer.str_to_bb(rowKey), colPath, ConsistencyLevel.ONE).
                getColumn();
        String value = "";
        value += "Temperature:" + MyByteBuffer.bb_to_str(col.value);

        colPath.setColumn(MyByteBuffer.str_to_bb(GlobalObjects.Cassandra.PressureColumn));
        col = client.get(MyByteBuffer.str_to_bb(rowKey), colPath, ConsistencyLevel.ONE).getColumn();
        value += "Pressure:" + MyByteBuffer.bb_to_str(col.value);
        return value;
    }


    /*
     * Creates new namespace
     */
    private boolean createNameSpace(String keySpace) {
        KsDef k = new KsDef();
        k.setName(keySpace);
        k.setReplication_factor(GlobalObjects.Cassandra.ReplicationFactor);
        //k.setStrategy_class("org.apache.cassandra.locator.RackUnawareStrategy");
        //Cassandra.Client client=this.getClient();
        try {
            this.client.system_add_keyspace(k);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return true;
    }

    /*
     * Gets the client object
     */
    private Cassandra.Client getClient() {
        try {
            this.client = Connect();
        } catch (Exception ex) {
            //ex.printStackTrace();
        }
        return this.client;
    }


    /*
     * Closes the client
     */
    private void closeClient() {
        Close();
    }


    /*
     * Opens the client
     */
    private void openClient() {
        try {
            if (!this.tr.isOpen()) {
                this.tr.open();
            }
        } catch (TTransportException ex) {
            ex.printStackTrace();
        }
    }

    private void sendStatusMessage(String message) {
        this.setChanged();
        this.notifyObservers(message);
    }

    /**
     * Checks the availabilities of files and directories
     */
    private void checkResultIO() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        String directoryName = "Result_" + dateFormat.format(calendar.getTime());
        File f = new File(directoryName);
        if (!f.exists()) {
            f.mkdir();
        }
        f = new File(directoryName + "/Cassandra.result");
        if (!f.exists()) {
            try {
                f.createNewFile();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Writes the result of insertion operation in csv file
     */
    private void insertResult(float size, float time) {
        System.out.println("Going to write....Filesize=" + size + " time=" + time);
        //this.csvWriter.writeNext(new String[]{"5",Float.toString(size), Float.toString(time)});
        System.out.println("Hurry I am here");
    }

    private void initialiseCSVWriter() {
        //Create directory
//        String directory = "Result";
//        File f = new File(directory);
//        if (!f.exists()) {
//            f.mkdir();
//            f.setWritable(true);
//        }
//        //Create file
//        f = new File(this.fileName);
//        if (!f.exists()) {
//            try {
//                f.createNewFile();
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        }
        //create csv writer
//        this.csvWriter=null;
//        try{
//            this.csvWriter=new CSVWriter(new FileWriter(this.fileName), ',');
//        }catch(IOException ex){
//           ex.printStackTrace();
//        }
    }
}