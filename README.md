Project to execute xquery using virtual partitioning and Hadoop cluster.

To build

$ ant

To run

$ java -jar dist/vphadoop.jar

NOTES:

The current implementation expects some default configurations in Hadoop cluster and the path used by Ant tu put the files. Please check the VPGui.java file and change it accordingly if you plan on executing this in your machine.
