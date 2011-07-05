package Distributed_Performance;

import java.awt.Toolkit;
import javax.swing.JFrame;

/**
 * This is startup class which creates the calendar frame
 * @author Krishna
 */
public class Main {    

    public static void main(String[] args) throws Exception {
         MainFrame frame=new MainFrame();
         frame.setVisible(true);
         frame.setState(JFrame.NORMAL);
         Toolkit toolKit=Toolkit.getDefaultToolkit();
         frame.setSize(toolKit.getScreenSize());
         frame.setResizable(false);
         frame.setVisible(true);
    }
}
