package Distributed_Performance;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.File;
import java.io.IOException;

/**
 * Exports the result in a file
 * @author krishna
 */
public class ResultExporter {

    //File name
    private CSVWriter csvWriter = null;

    /**
     * Constructor
     * @param fileName
     */
    public ResultExporter() {
        this.createDirectory();
    }

    private void createDirectory() {
        File f = new File("result");
        if (!f.exists()) {
            f.mkdir();
        }
    }

    /**
     * Opens the exporter
     * @throws IOException
     * @param fileName
     */
    public void open(String fileName) throws IOException {
        this.csvWriter = new CSVWriter(new java.io.FileWriter(fileName), ',');
    }

    /**
     * inserts the insertion information
     * @param megaBytes size in megabytes
     * @param seconds time in seconds
     */
    public void writeInsertionInfo(int megaBytes, float seconds) {
        this.csvWriter.writeNext(new String[]{Integer.toString(megaBytes), Float.toString(seconds)});
    }

    public void writeFetchInfo(GlobalObjects.DatabaseType databaseType, int dataLength, long averageFetchTime,
            long minimunTime, long maximumTime) {
        this.csvWriter.writeNext(new String[]{databaseType.toString(), Integer.toString(dataLength),
                    Long.toString(minimunTime), Long.toString(averageFetchTime), Long.toString(maximumTime)});
    }

    /**
     * Closes the exporter
     * @throws IOException
     */
    public void close() throws IOException {
        this.csvWriter.close();
    }
}
