package uff.dew.vphadoop.client;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.connector.VPInputFormat;
import uff.dew.vphadoop.job.MyMapper;
import uff.dew.vphadoop.job.MyReducer;

public class VPGui {
    
    private JFrame frame;
    
    private JTextField hostField;
    private JTextField portField;
    private JTextField usernameField;
    private JTextField passwordField;
    
    private JTextArea queryArea;
    private JTextArea outputArea;
    
    private JButton queryButton;
    
    public VPGui() {
        
        frame = new JFrame("VPHadoop");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(640,480);
        
        addContent(frame.getContentPane());
        
        addListeners();
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
        hostField = new JTextField("127.0.0.1");
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
        portField = new JTextField("1984");
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
        usernameField = new JTextField("admin");
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
        passwordField = new JTextField("admin");
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
        
        queryArea = new JTextArea();
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
        
        queryButton = new JButton("Execute!");
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
        
        outputArea = new JTextArea();
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
    
    private void addListeners() {
        queryButton.addActionListener(new ActionListener() {
            
            @Override
            public void actionPerformed(ActionEvent ae) {
                javax.swing.SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        startHadoopJob();                    }
                });

            }
        });
    }
    
    private void startHadoopJob() {
        
        Configuration conf = new Configuration();
        // TODO read this from interface
        conf.set("fs.default.name","hdfs://hadoop-dev:9000/");
        conf.set("mapred.job.tracker", "hadoop-dev:9001");
        
        // TODO read this from interface
        conf.set(VPConst.DB_DOCUMENT, "standard");
        conf.set(VPConst.DB_XQUERY, "/site/people/person/name/text()");
        conf.set(VPConst.DB_CONFIGFILE_PATH, "configuration.xml");
        
        try {
            writeDbConfiguration(conf);
            
            Job job = setupJob(conf);
            
            job.waitForCompletion(true);
        } catch (Exception e) {
            //TODO handle this exception nicely
            e.printStackTrace();
        }
    }
    
    private void writeDbConfiguration(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create("vphadoop"), conf);
        FSDataOutputStream out = fs.create(new Path("configuration.xml"),true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        bw.write("<?xml version=\"1.0\"?>\n");
        bw.write("<vphadoop>\n");
        bw.write("<database>\n");
        bw.write("<type>"+ "BASEX" +"</type>\n");
        bw.write("<host>"+ hostField.getText().trim()+"</host>\n");
        bw.write("<port>"+ portField.getText().trim()+"</port>\n");
        bw.write("<username>"+ usernameField.getText().trim()+"</username>\n");
        bw.write("<password>"+ passwordField.getText().trim()+"</password>\n");
        bw.write("</database>\n");
        bw.write("</vphadoop>\n");
        bw.close();
        out.close();
    }

    private Job setupJob(Configuration conf) throws IOException {
        
        String localJarsDir = "./dist";
        String hdfsJarsDir = "/user/hduser/libs";
        JobHelper.copyLocalJarsToHdfs(localJarsDir, hdfsJarsDir, conf);
        JobHelper.addHdfsJarsToDistributedCache(hdfsJarsDir, conf);
        Job job = new Job(conf,"vphadoop");
        
        job.setInputFormatClass(VPInputFormat.class);
        
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path("output");
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(outputPath)){
            dfs.delete(outputPath, true);
        }
        
        FileOutputFormat.setOutputPath(job, outputPath);
        
        return job;
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
