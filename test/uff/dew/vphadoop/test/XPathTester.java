package uff.dew.vphadoop.test;

import static org.junit.Assert.*;

import org.junit.Test;

import uff.dew.vphadoop.xquery.XPathExpression;

public class XPathTester {

    private static final String DOC = "standard";
    private static final String EXPR = "/nivel0/nivel1/nivel2/nivel3/nivel4";
    
    XPathExpression e = new XPathExpression(DOC,EXPR);
    
    @Test
    public void testGetSubPath() {
        
        assertEquals("should be equal", "doc('"+ DOC + "')"+ "/nivel0", e.getSubPath(0));
        assertEquals("should be equal", "doc('"+ DOC + "')"+ "/nivel0/nivel1", e.getSubPath(1));
        assertEquals("should be equal", "doc('"+ DOC + "')"+ "/nivel0/nivel1/nivel2", e.getSubPath(2));
        assertEquals("should be equal", "doc('"+ DOC + "')"+ EXPR, e.getSubPath(4));
        assertEquals("should be equal", "doc('"+ DOC + "')"+ EXPR, e.getSubPath(10));
    }

    @Test
    public void testGetFullPath() {
        assertEquals("should be equal", "doc('"+ DOC + "')"+ EXPR, e.getFullPath());
    }

    @Test
    public void testGetPathWithSelection() {
        assertEquals("should be equal", "doc('standard')/nivel0/nivel1[position()=1]/nivel2/nivel3/nivel4", 
                e.getPathWithSelection("position()=1", 1));
    }
    
    @Test
    public void testGetNumLevels() {
        assertEquals("should be equal", 4, e.getNumLevels());
    }

}
