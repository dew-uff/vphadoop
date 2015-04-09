package uff.dew.vphadoop.test;

import java.util.ArrayList;
import java.util.Collections;

public class ShuffleTest {

    public static void main(String[] args) {
        
        ArrayList<Integer> array = new ArrayList<Integer>();
        
        for (int i = 0; i < 20; i++) {
            array.add(i);
        }

        Collections.shuffle(array);
        
        for (int i = 0; i < 20; i++) {
            System.out.print(array.get(i) + ", ");
        }
    }
}
