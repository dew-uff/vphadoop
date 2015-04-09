package uff.dew.vphadoop.test;

import static org.junit.Assert.*;

import java.io.StringReader;

import mediadorxml.javaccparser.ParseException;
import mediadorxml.javaccparser.XQueryParser;

import org.junit.Test;

public class XQueryParserTest {

    private static final String FOR_XQUERY = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in doc('standard')/site/people/person \r\n"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    private static final String FOR_WHERE_XQUERY = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in doc('standard')/site/people/person \r\n"+
            "   where $p/address/country = \"United States\""+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "    {$country} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    private static final String FOR_LET_XQUERY = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in doc('standard')/site/people/person \r\n"+
            "   let $country := $p/address/country"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "    {$country} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    private static final String FOR_LET_GROUPBY_XQUERY = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in doc('standard')/site/people/person \r\n"+
            "   let $country := $p/address/country \r\n"+
            "   group by $country"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "    {$country} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>";     
    
    @Test
    public void testForQuery() {
        XQueryParser parser = new XQueryParser(new StringReader(FOR_XQUERY));
        try {
            assertNotNull(parser.Start());
        } catch (ParseException e) {
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void testForWhereQuery() {
        XQueryParser parser = new XQueryParser(new StringReader(FOR_WHERE_XQUERY));
        try {
            assertNotNull(parser.Start());
        } catch (ParseException e) {
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void testForLetQuery() {
        XQueryParser parser = new XQueryParser(new StringReader(FOR_LET_XQUERY));
        try {
            assertNotNull(parser.Start());
        } catch (ParseException e) {
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void testForLetGroupByQuery() {
        XQueryParser parser = new XQueryParser(new StringReader(FOR_LET_GROUPBY_XQUERY));
        try {
            assertNotNull(parser.Start());
        } catch (ParseException e) {
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

}
