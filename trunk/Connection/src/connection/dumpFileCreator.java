
package Distributed_Performance;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;
import java.io.File;
import java.io.IOException;
import java.util.Observable;
/**
 *The instance of this class creates dump file with the specified size
 * @author root
 */
public class dumpFileCreator extends Observable implements Runnable{

    public String fileName="";
    public int fileSize=0;
    public dumpFileCreator() {
    }

    /*
     * This creates the dump file with specified size
     * @param fileName Name of the file
     * @param fileSize Size in mb
     * @return the result of creation
     */
    public void createDumpFile(String fileName, int fileSize){
        this.fileName=fileName;
        this.fileSize=fileSize;
        Thread t=new Thread(this,"MyThread");
        t.start();
        //this.run();
    }
    @Override
    public void run(){
        this.notifyFinished("Start of creating "+this.fileSize+"MB dump file....");
        CSVWriter csvWriter=null;
        try{
            csvWriter=new CSVWriter(new FileWriter(fileName),',');
        }catch(IOException ex){
            ex.printStackTrace();
        }

        while(true){
            Random random=new Random();
            float temperature=random.nextFloat();
            float pressure=random.nextFloat();
            Date date=new Date();
            String[] contents={date.toString(),Float.toString(temperature),Float.toString(pressure)};
            //check if the file size is okay
            float size=0;
            File f=new File(fileName);
            size=f.length()/1024/1024;
            if(size<fileSize)
                csvWriter.writeNext(contents);
            else
            {
                break;
            }

            //Sleep here
            /*try{
                //Thread.sleep(1);
            }catch(Exception ex){
                //
            }*/
         }
         try{
            csvWriter.close();
            }catch(IOException ex){
                ex.printStackTrace();
            }
        this.notifyFinished("Successfully Finished.");
    }

    private void notifyFinished(String message){
        System.out.print(message);
        setChanged();
        this.notifyObservers(message);
        clearChanged();
    }

}
