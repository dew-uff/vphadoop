package uff.dew.vphadoop.ui;

import javax.swing.JFrame;

import org.apache.hadoop.conf.Configuration;

import uff.dew.vphadoop.VPConst;

public class VPTest {

    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONFIGFILE_PATH, args[0]);
        // TODO read this from a file, in a higher level
        conf.set(VPConst.DB_XQUERY, "/site/people/person/name/text()");
        conf.set(VPConst.DB_DOCUMENT, "standard");
        
//        JFrame frame = new JFrame("VP-Hadoop");
//        
//        setupFrame(frame);
//        
//        showFrame(frame);
    }

    private static void setupFrame(JFrame frame) {
        // TODO Auto-generated method stub
        
    }

}
