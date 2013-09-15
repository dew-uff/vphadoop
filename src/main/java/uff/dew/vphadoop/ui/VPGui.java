package uff.dew.vphadoop.ui;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.border.EtchedBorder;

public class VPGui {

    JFrame frame;
    
    public VPGui() {
        
        frame = new JFrame("VPHadoop");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(640,480);
        
        addContent(frame.getContentPane());
    }

    private void addContent(Container pane) {
        
        pane.setLayout(new GridBagLayout());

        addQueryArea(pane);
        addButtonArea(pane);
        addOutputArea(pane);
    }

    private void addQueryArea(Container pane) {
        JLabel label = new JLabel("Query");
        GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.5; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = 0;
        cl.insets = new Insets(10, 10, 0, 0);
        cl.anchor = GridBagConstraints.WEST;
        pane.add(label, cl);    
        JTextArea queryArea = new JTextArea();
        queryArea.setBorder(BorderFactory.createEtchedBorder(EtchedBorder.RAISED));

        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 0.5; 
        c.weighty = 0.5;
        c.gridx = 0;
        c.gridy = 1;
        c.insets = new Insets(10, 10, 10, 10);
        pane.add(queryArea,c);
    }

    private void addButtonArea(Container pane) {
        
        JButton queryButton = new JButton("Execute!");
        queryButton.setMaximumSize(new Dimension(150, 30));
        
        GridBagConstraints c = new GridBagConstraints();
        c.weighty = 0;
        c.gridx = 0;
        c.gridy = 2;
        c.anchor = GridBagConstraints.CENTER;
        pane.add(queryButton, c);
    }
    
    private void addOutputArea(Container pane) {
        JLabel label = new JLabel("Output");
        GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.5; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = 3;
        cl.insets = new Insets(10, 10, 0, 0);
        cl.anchor = GridBagConstraints.WEST;
        pane.add(label, cl);        
        
        JTextArea outputArea = new JTextArea();
        outputArea.setBorder(BorderFactory.createEtchedBorder(EtchedBorder.RAISED));
        
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 0.5; 
        c.weighty = 0.5;
        c.gridx = 0;
        c.gridy = 4;
        c.insets = new Insets(10, 10, 10, 10);
        pane.add(outputArea, c);
    }
    
    public void show() {
        frame.setVisible(true);
    }
       
    private static void createAndShowGUI() {

        VPGui gui = new VPGui();
        gui.show();
    }
    
    public static void main(String[] args) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI();
            }
        });
    }
}
