package connection;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.IOException;

/**
 * Exports the result in a file
 * @author krishna
 */
public class ResultExporter {

    //File name
    private CSVWriter csvWriter=null;
    /**
     * Constructor
     * @param fileName
     */
    public ResultExporter(){
    }

    /**
     * Opens the exporter
     * @throws IOException
     * @param fileName
     */
    public void open(String fileName) throws IOException{
        this.csvWriter=new CSVWriter(new java.io.FileWriter(fileName),',');
    }

    /**
     * inserts the insertion information
     * @param megaBytes size in megabytes
     * @param seconds time in seconds
     */
    public void writeInsertionInfo(int megaBytes,float seconds){
        this.csvWriter.writeNext(new String[]{Integer.toString(megaBytes), Float.toString(seconds)});
    }

    /**
     * Closes the exporter
     * @throws IOException
     */
    public void close() throws IOException{
        this.csvWriter.close();
    }

}
