package connection;

import au.com.bytecode.opencsv.CSVReader;
import java.util.List;
import java.io.FileReader;
import java.io.IOException;
import java.util.Observable;

/**
 * The instance of this class is for loading the dump data to memory
 * in list form
 * @author Krishna
 */
public class DumpFileLoader extends Observable {

    public List<String[]> SensorData = null;

    public DumpFileLoader() {
    }

    /**
     * Loads the dump file into memory
     * @param fileName file to load
     * @return list of string arrays
     */
    public List<String[]> loadFile(String fileName) {
        this.sendStatusMessage("Loading started.....");
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader(fileName), ',');
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {
            this.SensorData = reader.readAll();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        this.sendStatusMessage("Loading successfully finished.");
        return this.SensorData;
    }

    private void sendStatusMessage(String message) {
        this.setChanged();
        this.notifyObservers(message);
    }
}
