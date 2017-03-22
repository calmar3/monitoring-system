package org.apache.flink.quickstart;

/**
 * Created by maurizio on 21/03/17.
 */
public class Lamp {


    private long id;
    private double consumption;

    public Lamp() {}

    public Lamp(long id, double consumption){
        this.id = id;
        this.consumption = consumption;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public double getConsumption() {
        return consumption;
    }

    public void setConsumption(double consumption) {
        this.consumption = consumption;
    }





    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(",");
        sb.append(consumption);

        return sb.toString();
    }




    public static Lamp fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 2) {
            throw new RuntimeException("Invalid record: " + line);
        }

        Lamp ride = new Lamp();

        try {
            ride.id = Long.parseLong(tokens[0]);
            ride.consumption = Double.parseDouble(tokens[1]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

}
