package uff.dew.vphadoop.connector;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;


public class VPInputSplit extends InputSplit implements Writable {
    
    private List<String> queries;
	
	private int startPos = -1;


	private static String[] nodes = {"127.0.0.1"};
	
	public VPInputSplit() {
	    //TODO verify this
	}
	
	public VPInputSplit(int start, List<String> queries) {
	    this.queries = queries;
	}
	
    @Override
	public long getLength() throws IOException {
		// TODO
		return queries.size();
	}

	@Override
	public String[] getLocations() throws IOException {
		return nodes;
	}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(queries.size());
        for (int i = 0; i < queries.size(); i++) {
            out.writeUTF(queries.get(i));
            
        }
        out.writeInt(startPos);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int nqueries = in.readInt();
        queries = new ArrayList<String>(nqueries);
        for (int i = 0; i < nqueries; i++) {
            queries.add(in.readUTF());
        }
        startPos = in.readInt();
    }

    public Iterator<String> getQueriesIterator() {
        return queries.iterator();
    }
}
