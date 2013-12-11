package uff.dew.vphadoop.connector;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;


public class VPInputSplit extends InputSplit implements Writable {
    
    private String query;
	
	private int startPos = -1;


	private static String[] nodes = {"127.0.0.1"};
	
	public VPInputSplit() {
	    //TODO verify this
	}
	
	public VPInputSplit(int start, String query) {
		this.startPos = start;
	    this.query = query;
	}
	
    @Override
	public long getLength() throws IOException {
		// TODO
		return 1;
	}

	@Override
	public String[] getLocations() throws IOException {
		return nodes;
	}

	public int getStartPosition() {
		return startPos;
	}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(query);
        out.writeInt(startPos);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        query = in.readUTF();
        startPos = in.readInt();
    }

    public String getFragmentQuery() {
        return query;
    }
}
