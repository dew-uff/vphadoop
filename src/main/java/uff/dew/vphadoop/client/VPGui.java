package uff.dew.vphadoop.client;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.StringReader;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;

import mediadorxml.javaccparser.XQueryParser;
import uff.dew.vphadoop.client.runner.HadoopJobRunner;
import uff.dew.vphadoop.client.runner.JobListener;
import uff.dew.vphadoop.client.runner.JobRunner;
import uff.dew.vphadoop.db.DatabaseFactory;

public class VPGui implements JobListener {
    
	private static final int HADOOP_CONF_LABEL_POS = 0;
	private static final int HADOOP_CONF_LINE_POS = HADOOP_CONF_LABEL_POS + 1;
	private static final int DB_CONF_LABEL_POS = HADOOP_CONF_LINE_POS + 1;
	private static final int DB_CONF_LINE_POS = DB_CONF_LABEL_POS + 1;
	private static final int DB_CONF_TYPE_LINE = DB_CONF_LINE_POS + 1;
	private static final int QUERY_LABEL_POS = DB_CONF_TYPE_LINE + 1;
	private static final int QUERY_POS = QUERY_LABEL_POS + 1;
	private static final int BUTTON_LINE_POS = QUERY_POS + 1;
	private static final int OUTPUT_LABEL_POS = BUTTON_LINE_POS + 1;
	private static final int OUTPUT_POS = OUTPUT_LABEL_POS + 1;
	private static final int MAP_PROGRESS_POS = OUTPUT_POS + 1;
	private static final int REDUCE_PROGRESS_POS = MAP_PROGRESS_POS + 1;
	
    // c1
    //private static final String FIXED_QUERY = "<results> { for $c in doc('dblp')/dblp/inproceedings where $c/year >1984 and $c/year <=2007 return <inproceeding> {$c/title} </inproceeding> } </results>"; 
    
    // c11
    private static final String FIXED_QUERY = "<results> {   for $it in doc('auction')/site/regions/africa/item   for $co in doc('auction')/site/closed_auctions/closed_auction   where $co/itemref/@item = $it/@id   and $it/payment = \"Cash\"    return     <itens>      {$co/price}      {$co/date}      {$co/quantity}      {$co/type}      {$it/payment}      {$it/location}      {$it/from}      {$it/to}    </itens> }</results>";
    
    // c12
    //private static final String FIXED_QUERY = "<results> {   for $op in doc('xmlDataBaseXmark')/site/open_auctions/open_auction   let $bd := $op/bidder where count($op/bidder) > 5   return      <open_auctions_with_more_than_5_bidders>        <auction>           {$op}        </auction>        <qty_bidder>           {count($op/bidder)}        </qty_bidder>     </open_auctions_with_more_than_5_bidders>}</results>";
    
    // c5
    //private static final String FIXED_QUERY = "<results> { for $it in doc('xmlDataBaseXmark')/site/regions/asia/item for $pe in doc('xmlDataBaseXmark')/site/people/person where $pe/profile/interest/@category = $it/incategory/@category return <people> {$pe} </people> } </results>";
    
    private JFrame frame;
    
    private JTextField hostField;
    private JTextField portField;
    private JTextField usernameField;
    private JTextField passwordField;
	private JTextField jobTrackerHostField;
	private JTextField jobTrackerPortField;
	private JTextField namenodeHostField;
	private JTextField namenodePortField;
	private JTextField dbNameField;
    
    private JTextArea queryArea;
    private JTextArea outputArea;
    
    private JButton queryButton;
    private JCheckBox outputCheckbox;
    private ButtonGroup dbTypeChooser;
    
    private JProgressBar mapProgress;
    private JProgressBar reduceProgress;
    
    private JobRunner jobRunner;
    
    public VPGui() {
        
        frame = new JFrame("VPHadoop");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800,600);
        
        addContent(frame.getContentPane());
        
        addListeners();
    }

    private void addContent(Container pane) {
        
        pane.setLayout(new GridBagLayout());
        addHadoopConfigArea(pane);
        addDBConfigArea(pane);
        addQueryArea(pane);
        addButtonArea(pane);
        addOutputArea(pane);
        addStatusArea(pane);
    }
    
    private void addHadoopConfigArea(Container pane) {
        
    	GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.0; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = HADOOP_CONF_LABEL_POS;
        cl.gridwidth = 8;
        cl.insets = new Insets(10, 10, 0, 0);
        cl.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("Hadoop Configuration"), cl);
        
        // jobtracker host
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 0;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("Jobtracker"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 1;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 0);
        jobTrackerHostField = new JTextField("127.0.0.1");
        jobTrackerHostField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(jobTrackerHostField,c);
        
        // jobtracker port
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 2;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("port"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 3;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 0);
        jobTrackerPortField = new JTextField("54310");
        jobTrackerPortField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(jobTrackerPortField,c);
        
        // namenode host
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 4;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("Namenode"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 5;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 0);
        namenodeHostField = new JTextField("127.0.0.1");
        namenodeHostField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(namenodeHostField,c);
        
        // namenode port
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 6;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("port"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 7;
        c.gridy = HADOOP_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 10);
        namenodePortField = new JTextField("54311");
        namenodePortField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(namenodePortField,c);
    }

    private void addDBConfigArea(Container pane) {

        GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.0; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = DB_CONF_LABEL_POS;
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
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("host"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 1;
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 0);
        hostField = new JTextField("127.0.0.1");
        hostField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(hostField,c);
        
        // port
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 2;
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("port"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 3;
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 0);
        portField = new JTextField("1984");
        portField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(portField,c);
        
        // username
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 4;
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("username"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 5;
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 0);
        usernameField = new JTextField("admin");
        usernameField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(usernameField,c);
        
        // password
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 6;
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("password"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 7;
        c.gridy = DB_CONF_LINE_POS;
        c.insets = new Insets(10, 0, 0, 10);
        passwordField = new JTextField("admin");
        passwordField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(passwordField,c);
        
        // db type
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 1;
        c.gridy = DB_CONF_TYPE_LINE;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("DB type"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 2;
        c.gridy = DB_CONF_TYPE_LINE;
        c.gridwidth = 2;
        c.insets = new Insets(10, 0, 0, 10);
        dbTypeChooser = new ButtonGroup();
        final JRadioButton basexButton = new JRadioButton("BaseX");
        basexButton.setActionCommand(DatabaseFactory.TYPE_BASEX);
        basexButton.setSelected(true);
        basexButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (basexButton.isSelected()) {
                    usernameField.setText("admin");
                    passwordField.setText("admin");
                    portField.setText("1984");
                    dbNameField.setEnabled(false);
                }
            }
        });

        final JRadioButton sednaButton = new JRadioButton("Sedna");
        sednaButton.setActionCommand(DatabaseFactory.TYPE_SEDNA);
        sednaButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (sednaButton.isSelected()) {
                    usernameField.setText("SYSTEM");
                    passwordField.setText("MANAGER");
                    portField.setText("5050");
                    dbNameField.setEnabled(true);
                }
            }
        });

        dbTypeChooser.add(basexButton);
        dbTypeChooser.add(sednaButton);
        
        JPanel radioPanel = new JPanel(new GridLayout(1,2));
        radioPanel.add(basexButton);
        radioPanel.add(sednaButton);
        pane.add(radioPanel,c);
        
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.0; 
        cl.weighty = 0.0;
        cl.gridx = 4;
        cl.gridy = DB_CONF_TYPE_LINE;
        cl.gridwidth = 1;
        cl.insets = new Insets(10, 10, 0, 0);
        cl.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("Database name"), cl);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 5;
        c.gridy = DB_CONF_TYPE_LINE;
        c.insets = new Insets(10, 0, 0, 10);
        dbNameField = new JTextField("regular");
        dbNameField.setEnabled(false);
        dbNameField.setHorizontalAlignment(JTextField.CENTER);
        pane.add(dbNameField,c);
    }

    private void addQueryArea(Container pane) {
        JLabel label = new JLabel("Query");
        GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.5; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = QUERY_LABEL_POS;
        cl.gridwidth = 8;
        cl.insets = new Insets(10, 10, 0, 0);
        cl.anchor = GridBagConstraints.WEST;
        pane.add(label, cl);
        
        queryArea = new JTextArea(FIXED_QUERY);
        JScrollPane scrollQuery = new JScrollPane(queryArea);
        scrollQuery.setBorder(BorderFactory.createEtchedBorder(EtchedBorder.RAISED));
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 0.5; 
        c.weighty = 0.5;
        c.gridx = 0;
        c.gridy = QUERY_POS;
        c.gridwidth = 8;
        c.insets = new Insets(10, 10, 10, 10);
        pane.add(scrollQuery,c);
    }

    private void addButtonArea(Container pane) {
        
        queryButton = new JButton("Execute!");
        queryButton.setMaximumSize(new Dimension(150, 30));
        
        GridBagConstraints c = new GridBagConstraints();
        c.weighty = 0;
        c.gridx = 0;
        c.gridy = BUTTON_LINE_POS;
        c.gridwidth = 8;
        c.anchor = GridBagConstraints.CENTER;
        pane.add(queryButton, c);
    }
    
    private void addOutputArea(Container pane) {
        outputCheckbox = new JCheckBox("Output",true);
        
        //JLabel label = new JLabel("Output");
        GridBagConstraints cl = new GridBagConstraints();
        cl.fill = GridBagConstraints.NONE;
        cl.weightx = 0.5; 
        cl.weighty = 0.0;
        cl.gridx = 0;
        cl.gridy = OUTPUT_LABEL_POS;
        cl.gridwidth = 8;
        cl.insets = new Insets(10, 10, 0, 0);
        cl.anchor = GridBagConstraints.WEST;
        pane.add(outputCheckbox, cl);        
        
        outputArea = new JTextArea();
        JScrollPane scrollOutput = new JScrollPane(outputArea);
        scrollOutput.setBorder(BorderFactory.createEtchedBorder(EtchedBorder.RAISED));
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 0.5; 
        c.weighty = 0.5;
        c.gridx = 0;
        c.gridy = OUTPUT_POS;
        c.gridwidth = 8;
        c.insets = new Insets(10, 10, 10, 10);
        pane.add(scrollOutput, c);
    }
    
    private void addStatusArea(Container pane) {
       
        // map
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 0;
        c.gridy = MAP_PROGRESS_POS;
        c.gridwidth = 2;
        c.insets = new Insets(10, 10, 0, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("Map progress"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 2;
        c.gridy = MAP_PROGRESS_POS;
        c.gridwidth = 6;
        c.insets = new Insets(10, 0, 0, 10);
        mapProgress = new JProgressBar(0,100);
        pane.add(mapProgress,c);

        // reduce
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0.0; 
        c.weighty = 0.0;
        c.gridx = 0;
        c.gridy = REDUCE_PROGRESS_POS;
        c.gridwidth = 2;
        c.insets = new Insets(10, 10, 10, 0);
        c.anchor = GridBagConstraints.WEST;
        pane.add(new JLabel("Reduce progress"), c);
        
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        c.weighty = 0.0;
        c.gridx = 2;
        c.gridy = REDUCE_PROGRESS_POS;
        c.gridwidth = 6;
        c.insets = new Insets(10, 0, 10, 10);
        reduceProgress = new JProgressBar(0,100);
        pane.add(reduceProgress,c);
    }
    
    private void addListeners() {
        queryButton.addActionListener(new ActionListener() {
            
            @Override
            public void actionPerformed(ActionEvent ae) {
                
                if (checkXquery()) {
                    javax.swing.SwingUtilities.invokeLater(new Runnable() {
                        @Override
                        public void run() {
                            startHadoopJob();                    
                        }
                    });
                }

            }
        });
    }

    private boolean checkXquery() {
        
        boolean result = false;
        XQueryParser parser = new XQueryParser(new StringReader(queryArea.getText().trim()));
        try {
            parser.Start();
            result = true;
        } 
        catch (Exception pe) {
            outputArea.setText(pe.getMessage());
        }
        return result;
    }
    
    private void startHadoopJob() {
        
       try { 
            HadoopJobRunner hadoopJob = new HadoopJobRunner(queryArea.getText().trim());
            
            hadoopJob.setHadoopConfiguration(jobTrackerHostField.getText().trim(), Integer.parseInt(jobTrackerPortField.getText().trim()), 
            		namenodeHostField.getText().trim(), Integer.parseInt(namenodePortField.getText().trim()));
            String dbType = dbTypeChooser.getSelection().getActionCommand();
            
            hadoopJob.setDbConfiguration(dbType,hostField.getText().trim(), Integer.parseInt(portField.getText().trim()), 
                    usernameField.getText().trim(), passwordField.getText().trim(), dbNameField.getText().trim());
            
            hadoopJob.addListener(this);
            
            hadoopJob.runJob();
            
            queryButton.setEnabled(false);
            mapProgress.setValue(0);
            mapProgress.setString("");
            reduceProgress.setValue(0);
            reduceProgress.setString("");
            outputArea.setText("");

            jobRunner = hadoopJob;
            
        } catch (Exception e) {
            JOptionPane.showMessageDialog(frame, "Well.. something is not right!\n"+e.getMessage(), 
                    "Error", JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
    }
    
    private void showOutput() {
        outputArea.setText(jobRunner.getResult());
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

    @Override
    public void mapProgressChanged(int value) {
        if (value >= 0) {
            mapProgress.setValue(value);
        }
        
    }

    @Override
    public void reduceProgressChanged(int value) {
        if (value >= 0) {
            reduceProgress.setValue(value);
        }
    }

    @Override
    public void completed(boolean successful) {
        if (successful) {
            if (outputCheckbox.isSelected()) {
                showOutput();
            }
        }
        else {
            outputArea.setText("Erro!!!");
        }
        queryButton.setEnabled(true);
    }
}
