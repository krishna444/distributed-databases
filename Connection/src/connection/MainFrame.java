package connection;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;
import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.WindowConstants;

/**
 *
 * @author krishna
 */
public class MainFrame extends JFrame implements Observer {

    /**
     * @param args the command line arguments
     */
    static int id = 0;
    CassandraConnector cassandra = new CassandraConnector();
    MongoDb mongoDb = new MongoDb();
    Hypertable hyperTable = new Hypertable();
    HBase hbase = new HBase();
    dumpFileCreator creator = new dumpFileCreator();
    DumpFileLoader loader = new DumpFileLoader();
    JTextArea resultTextArea;

    public MainFrame() {
        super("Database Test Application");
        this.initialiseComponents();
    }

    private void initialiseComponents() {
        this.creator.addObserver(this);
        this.cassandra.addObserver(this);
        this.loader.addObserver(this);
        //Menu Operations
        JMenuBar menuBar = new JMenuBar();
        JMenu menu = new JMenu("Sensor Data");
        menuBar.add(menu);
        JMenu dumpSubMenu = new JMenu("Dump Data");
        JMenuItem dump32 = new JMenuItem("32MB");
        dump32.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                appendStatusMessage("Started");
                creator.createDumpFile("Dump_32.dmp", 32);

            }
        });

        JMenuItem dump64 = new JMenuItem("64MB");

        dump64.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                creator.createDumpFile("Dump_64.dmp", 64);
            }
        });
        JMenuItem dump128 = new JMenuItem("128MB");
        dump128.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                creator.createDumpFile("Dump_128.dmp", 128);
            }
        });
        JMenuItem dump256 = new JMenuItem("256MB");
        dump256.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                creator.createDumpFile("Dump_256.dmp", 256);
            }
        });
        JMenuItem dump512 = new JMenuItem("512MB");
        dump512.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                creator.createDumpFile("Dump_512.dmp", 512);
            }
        });
        JMenuItem dump1024 = new JMenuItem("1024MB");
        dump1024.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                creator.createDumpFile("Dump_1024.dmp", 1024);
            }
        });
        dumpSubMenu.add(dump32);
        dumpSubMenu.add(dump64);
        dumpSubMenu.add(dump128);
        dumpSubMenu.add(dump256);
        dumpSubMenu.add(dump512);
        dumpSubMenu.add(dump1024);

        JMenuItem exitMenuItem = new JMenuItem("Exit");
        exitMenuItem.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                System.exit(0);
            }
        });
        menu.add(dumpSubMenu);
        menu.add(exitMenuItem);


        JPanel topPanel = new JPanel(new FlowLayout());
        JPanel loadPanel = new JPanel(new FlowLayout());
        Object[] sizes = {"32MB", "64MB", "128MB", "256MB", "512MB", "1024MB"};
        final JComboBox sizeList = new JComboBox(sizes);
        JButton loadButton = new JButton("Load");
        loadButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                int selectedIndex = sizeList.getSelectedIndex();
                loader.loadFile(getFileName(selectedIndex));
            }
        });



        loadPanel.add(sizeList);
        loadPanel.add(loadButton);
        loadPanel.setBorder(BorderFactory.createTitledBorder("Load"));
        topPanel.add(loadPanel);


        JPanel insertButtonPanel = new JPanel(new FlowLayout());
        JButton insertCassandra = new JButton("Cassandra");
        insertCassandra.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                //insert the data into cassandra database
                cassandra.insert(loader.SensorData,getFileSize(sizeList.getSelectedIndex()));
            }
        });
        JButton insertMongoDb = new JButton("MongoDb");
        insertMongoDb.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                //insert the data into cassandra database
                Iterator<String[]> iterator = loader.SensorData.iterator();
                int rowCount = 0;
                while (iterator.hasNext()) {
                    String[] data = iterator.next();
                    System.out.println(rowCount + " temp=" + data[1] + " pressure=" + data[2]);
                    GlobalObjects.SensorData sensor = new GlobalObjects.SensorData(data[0], Float.parseFloat(data[1]), Float.parseFloat(data[2]));
                    try {
                        mongoDb.insertData(sensor);
                    } catch (Exception ex) {
                        //
                        System.out.println(ex.toString());
                        return;
                    }
                    rowCount++;
                }
            }
        });
        JButton insertHyperTable = new JButton("HyperTable");
        insertHyperTable.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                //insert the data into cassandra database
                Iterator<String[]> iterator = loader.SensorData.iterator();
                int rowCount = 0;
                while (iterator.hasNext()) {
                    String[] data = iterator.next();
                    System.out.println(rowCount + " temp=" + data[1] + " pressure=" + data[2]);
                    GlobalObjects.SensorData sensor = new GlobalObjects.SensorData(data[0], Float.parseFloat(data[1]), Float.parseFloat(data[2]));
                    try {
                        hyperTable.insertSensorData("row" + rowCount, sensor);
                    } catch (Exception ex) {
                        //
                        System.out.println(ex.toString());
                        return;
                    }
                    rowCount++;
                }
            }
        });
        JButton insertHBase = new JButton("HBase");
        insertHBase.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                Iterator<String[]> iterator = loader.SensorData.iterator();
                int rowCount = 0;
                while (iterator.hasNext()) {
                    String[] data = iterator.next();
                    System.out.println(rowCount + " temp=" + data[1] + " pressure=" + data[2]);
                    GlobalObjects.SensorData sensor = new GlobalObjects.SensorData(data[0], Float.parseFloat(data[1]), Float.parseFloat(data[2]));
                    try {
                        hbase.insertSensorData("row" + rowCount, sensor);
                    } catch (Exception ex) {
                        //
                        System.out.println(ex.toString());
                        return;
                    }
                    rowCount++;
                }
            }
        });


        insertButtonPanel.add(insertCassandra);
        insertButtonPanel.add(insertMongoDb);
        insertButtonPanel.add(insertHyperTable);
        insertButtonPanel.add(insertHBase);
        insertButtonPanel.setBorder(BorderFactory.createTitledBorder("Insert operations"));
        topPanel.add(insertButtonPanel);

        JPanel queryButtonPanel = new JPanel(new FlowLayout());
        ButtonGroup group = new ButtonGroup();
        JRadioButton cassandraRadioButton = new JRadioButton("Cassandra");
        cassandraRadioButton.setSelected(true);
        JRadioButton mongoDbRadioButton = new JRadioButton("Mongo");
        mongoDbRadioButton.setSelected(false);
        JRadioButton sqlClusterRadioButton = new JRadioButton("Hypertable");
        sqlClusterRadioButton.setSelected(false);
        group.add(cassandraRadioButton);
        group.add(mongoDbRadioButton);
        group.add(sqlClusterRadioButton);
        queryButtonPanel.add(cassandraRadioButton);
        queryButtonPanel.add(mongoDbRadioButton);
        queryButtonPanel.add(sqlClusterRadioButton);
        JLabel labelClient = new JLabel("Clients");
        Object[] values = {10, 100, 500, 1000, 5000, 10000};
        JComboBox comboBox = new JComboBox(values);
        JButton runButton = new JButton("Run");
        runButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        });
        queryButtonPanel.add(labelClient);
        queryButtonPanel.add(comboBox);
        queryButtonPanel.add(runButton);
        queryButtonPanel.setBorder(BorderFactory.createTitledBorder("Query Operations"));
        topPanel.add(queryButtonPanel);



        JPanel resultPanel = new JPanel(new BorderLayout());
        JLabel labelResult = new JLabel("Result Window:");
        resultPanel.add(labelResult, BorderLayout.NORTH);
        this.resultTextArea = new JTextArea(20, 30);
        resultPanel.add(resultTextArea, BorderLayout.CENTER);

        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        add(topPanel, BorderLayout.NORTH);
        add(resultPanel, BorderLayout.CENTER);
        setJMenuBar(menuBar);
    }

    @Override
    public void update(Observable observable, Object object) {
        if (observable == this.creator) {
            this.appendStatusMessage(object.toString());
        }
        if (observable == this.cassandra) {
            this.appendStatusMessage(object.toString());
        }
        if (observable == this.loader) {
            this.appendStatusMessage(object.toString());
        }
    }

    /**
     * Gets the filename with the specified index from chosen memory
     * @param index Index chosen
     * @return dump file name
     */
    private String getFileName(int index) {
        String fileName = "";
        switch (index) {
            case 0:
                fileName = "Dump_32.dmp";
                break;
            case 1:
                fileName = "Dump_64.dmp";
                break;
            case 2:
                fileName = "Dump_128.dmp";
                break;
            case 3:
                fileName = "Dump_256.dmp";
                break;
            case 4:
                fileName = "Dump_512.dmp";
                break;
            case 5:
                fileName = "Dump_1024.dmp";
                break;
            default:
                fileName = "Dump_32.dmp";
                break;
        }
        return fileName;
    }

    /**
     * Gets the filesize
     * @param index index
     * @return file size
     */
    private int getFileSize(int index){
        int fileSize=0;
        switch(index){
            case 0:
                fileSize=32;
                break;
            case 1:
                fileSize=64;
                break;
            case 2:
                fileSize=128;
                break;
            case 3:
                fileSize=256;
                break;
            case 4:
                fileSize=512;
                break;
            case 5:
                fileSize=1024;
                break;
            default:
                fileSize=32;
                break;
        }
        return fileSize;
    }

    private void appendStatusMessage(String message) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        String dateString = format.format(Calendar.getInstance().getTime());
        String status = dateString;
        status += "\t" + message + "\n";
        status += this.resultTextArea.getText();
        this.resultTextArea.setText(status);
        this.resultTextArea.repaint();
    }
}