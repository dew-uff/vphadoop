package uff.dew.svp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ConcatenationStrategy implements CompositionStrategy {
    
    private OutputStream output;
    
    public ConcatenationStrategy(OutputStream output) {
        this.output = output;
    }

    @Override
    public void loadPartial(InputStream partial) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void combinePartials() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        
    }

}
