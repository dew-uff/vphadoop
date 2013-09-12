package uff.dew.vphadoop.ui;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JTextArea;

public class VPGui {

    JFrame frame;
    
    public VPGui() {
        
        frame = new JFrame("VPHadoop");
        
        setupFrame();
    }

    private void setupFrame() {
        
        frame.setSize(640, 480);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        
        JTextArea queryArea = new JTextArea();
        JButton queryButton = new JButton();
        
        JTextArea outputArea = new JTextArea();
    }
    
    
    
    
}
