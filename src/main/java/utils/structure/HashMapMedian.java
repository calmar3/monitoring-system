package utils.structure;

import java.util.HashMap;

/**
 * Created by maurizio on 21/04/17.
 */
public class HashMapMedian {

    private static HashMap<String, Double> instance = null;

    protected HashMapMedian(){}

    public synchronized static final HashMap<String, Double> getInstance(){

        if(instance == null)
            instance = new HashMap<>();

        return instance;
    }
}
