
package connection;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.EventListener;
import java.util.EventObject;
import java.util.Random;
import javax.swing.event.EventListenerList;

/**
 *
 * @author root
 */
public class SensorGenerator {
    EventListenerList eventListenerList=new EventListenerList();

        public SensorGenerator() {

        }
        public void start() {
            test();
        }


     public void test(){
         int i=0;
         CSVWriter csvWriter=null;
            try{
                csvWriter=new CSVWriter(new FileWriter("Dump_56.dmp"),'\t');
            }catch(IOException ex){
                ex.printStackTrace();
            }
        while(true){
         GlobalObjects.SensorData sensor=new GlobalObjects.SensorData();
            Random random=new Random();
            float temperature=random.nextFloat();
            float pressure=random.nextFloat();
            Date date=new Date();
            sensor.date=date.toString();
            sensor.temperature=temperature;
            sensor.pressure=pressure;
            //SensorEvent sensorEvent=new SensorEvent(sensor);
            //this.fireEvent(sensorEvent);            
            String[] contents={sensor.date,Float.toString(sensor.temperature),Float.toString(pressure)};
            //check if the file size is okay
            float size=0;
            File f=new File("Dump_56.dmp");
            size=f.length()/1024/1024;
            if(size<56)
                csvWriter.writeNext(contents);
            else
            {
                try{
                csvWriter.close();
                }catch(Exception ex){
                    ex.printStackTrace();
                }
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


    }
      public void fireEvent(SensorEvent e){
        SensorListener[] listeners=eventListenerList.getListeners(SensorListener.class);
        System.out.print(listeners.length);
        for(int i=0;i<listeners.length;i++){
             listeners[i].handleSensorData(e);
        }
    }
     public void addCustomListener(SensorListener listener){
        this.eventListenerList.add(SensorListener.class, listener);
    }
    /*
     * Removes Listener
     */
    public void removeCustomListener(SensorListener listener){
        this.eventListenerList.remove(SensorListener.class, listener);
    }
    /*
     * Interface to handle the event
     */
    interface SensorListener extends EventListener{
        public void handleSensorData(SensorEvent e);
    }
    /*
     * Sensor event
     */
    class SensorEvent extends EventObject{

        GlobalObjects.SensorData sensorData=new GlobalObjects.SensorData();
        public SensorEvent(GlobalObjects.SensorData sensorData) {
            super(new Object());
            this.sensorData=sensorData;
        }


    }

}
