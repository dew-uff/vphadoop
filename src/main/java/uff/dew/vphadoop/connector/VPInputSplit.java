package uff.dew.vphadoop.connector;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;


public class VPInputSplit extends InputSplit implements Writable {
	
	private int startPos = -1;
	private int length = -1;
	private int level = -1;

	private static String[] nodes = {"127.0.0.1"};
	
	public VPInputSplit() {
	    //TODO verify this
	}
	
	public VPInputSplit(int start, int length, int level) {
		this.startPos = start;
		this.length = length;
		this.level = level;
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
	
	public int getLengh() {
		return length;
	}
	
	public int getSelectionLevel() {
	    return level;
	}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(startPos);
        out.writeInt(length);
        out.writeInt(level);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startPos = in.readInt();
        length = in.readInt();
        level = in.readInt();
    }
    
    
}
