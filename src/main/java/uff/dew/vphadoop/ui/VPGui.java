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
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;

public class VPGui {

    private JFrame frame;
    
    public VPGui() {
        
        frame = new JFrame("VPHadoop");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(640,480);
        
        addContent(frame.getContentPane());
    }

    private void addContent(Container pane) {
        
        pane.setLayout(new GridBagLayout());

        addDBConfigArea(pane);
        addQueryArea(pane);
        addButtonArea(pane);
        addOutputArea(pane);
    }

    private void addDBConfigArea(Container pane) {

        GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.0; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = 0;
        cl.gridwidth = 8;
        cl.insets = new Insets(10, 10, 0, 0);
        cl.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("DB Configuration"), cl);
        
        // host
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 0;
        c.gridy = 1;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("host"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 1;
        c.gridy = 1;
        c.insets = new Insets(10, 0, 0, 0);
        JTextField hostField = new JTextField("127.0.0.1");
        hostField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(hostField,c);
        
        // port
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 2;
        c.gridy = 1;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("port"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 3;
        c.gridy = 1;
        c.insets = new Insets(10, 0, 0, 0);
        JTextField portField = new JTextField("1984");
        portField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(portField,c);
        
        // username
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 4;
        c.gridy = 1;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("username"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 5;
        c.gridy = 1;
        c.insets = new Insets(10, 0, 0, 0);
        JTextField usernameField = new JTextField("admin");
        usernameField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(usernameField,c);
        
        // password
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 6;
        c.gridy = 1;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("password"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 7;
        c.gridy = 1;
        c.insets = new Insets(10, 0, 0, 10);
        JTextField passwordField = new JTextField("admin");
        passwordField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(passwordField,c);
    }

    private void addQueryArea(Container pane) {
        JLabel label = new JLabel("Query");
        GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.5; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = 2;
        cl.gridwidth = 8;
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
        c.gridy = 3;
        c.gridwidth = 8;
        c.insets = new Insets(10, 10, 10, 10);
        pane.add(queryArea,c);
    }

    private void addButtonArea(Container pane) {
        
        JButton queryButton = new JButton("Execute!");
        queryButton.setMaximumSize(new Dimension(150, 30));
        
        GridBagConstraints c = new GridBagConstraints();
        c.weighty = 0;
        c.gridx = 0;
        c.gridy = 4;
        c.gridwidth = 8;
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
        cl.gridy = 5;
        cl.gridwidth = 8;
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
        c.gridy = 6;
        c.gridwidth = 8;
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
