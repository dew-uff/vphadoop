package uff.dew.svp.strategy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;

public class ConcatenationStrategy implements CompositionStrategy {
    
    private OutputStream output;
    private boolean headerWritten;
    
    public ConcatenationStrategy(OutputStream output) {
        this.output = output;
        this.headerWritten = false;
    }

    @Override
    public void loadPartial(InputStream partial) throws IOException {
        
        BufferedReader br = new BufferedReader(new InputStreamReader(partial));

        SubQuery sq = SubQuery.getUniqueInstance(true);
        String constructorBegin = sq.getConstructorElement();
        String constructorEnd =  sq.getConstructorElement().replace("<", "</");
        
        boolean writeToOutput = false;
        String line = null;
        while((line = br.readLine()) != null) {
            if (line.indexOf(constructorBegin) != -1) {
                if (!headerWritten) {
                    output.write(SubQuery.getTitle().getBytes());
                    String header = constructorBegin + "\r\n";
                    output.write(header.getBytes());
                    headerWritten = true;
                }
                writeToOutput = true;
            } 
            else if (line.indexOf(constructorEnd) != -1 ||
                     line.indexOf(SubQuery.PARTIAL_IDORDEM_BEGIN_ELEMENT) != -1) {
                writeToOutput = false;
                break;
            }
            else if (writeToOutput) {
                output.write(line.getBytes());
                output.write("\r\n".getBytes());
            }
        }
    }

    @Override
    public void combinePartials() throws IOException {

        if (headerWritten) {
            // means we got partials written to the output. need to writter footer.
            SubQuery sq = SubQuery.getUniqueInstance(true);
            String constructorEnd =  sq.getConstructorElement().replace("<", "</");
            output.write(constructorEnd.getBytes());
            output.write("\r\n".getBytes());
        }
    }

    @Override
    public void cleanup() {
        // nothing to clean.
    }
}
