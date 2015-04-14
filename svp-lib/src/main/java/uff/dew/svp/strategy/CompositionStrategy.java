package uff.dew.svp.strategy;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author gabriel
 *
 */
public interface CompositionStrategy {
    
    public void loadPartial(InputStream partial) throws IOException;

    public void combinePartials() throws IOException;
    
    public void cleanup();
}
