package org.apache.flink.quickstart;

import java.util.HashMap;

/**
 * Created by marco on 25/03/17.
 */
public final class LampsAvl {


    /*Will be AVL instead HashMap*/
    private static HashMap<Long,Integer> allowedLamps;


    public static HashMap<Long,Integer> getInstance() {
        if (allowedLamps == null) {
            synchronized(LampsAvl.class) {
                if (allowedLamps == null) {
                    allowedLamps = new HashMap<>();
                }
            }
        }
        return allowedLamps;
    }


}
