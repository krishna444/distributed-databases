package connection;

import java.io.File;

/**
 *
 * @author krishna
 */
public class FileWriter {

    public static void main(String[] args) {
        File f = new File("test.txt");
        try {
            java.io.FileWriter fw = new java.io.FileWriter(f);
            fw.write("A quick brown fox jumps over the lazy dog");
            fw.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
