package model;

/**
 * Created by maurizio on 21/03/17.
 */
public class Lamp {


    private long lampId;
    private double consumption;
    private String address;

    public Lamp() {}

    public Lamp(long lampId, double consumption){
        this.lampId = lampId;
        this.consumption = consumption;
    }

    public Lamp(long lampId, double consumption,String address){
        this.lampId = lampId;
        this.consumption = consumption;
        this.address = address;
    }

    public long getId() {
        return lampId;
    }

    public void setId(long lampId) {
        this.lampId = lampId;
    }

    public double getConsumption() {
        return consumption;
    }

    public void setConsumption(double consumption) {
        this.consumption = consumption;
    }





    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.lampId).append(",");
        sb.append(this.consumption).append(",");
        sb.append(this.address);

        return sb.toString();
    }




    public static Lamp fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 2) {
            throw new RuntimeException("Invalid record: " + line);
        }

        Lamp ride = new Lamp();

        try {
            ride.lampId = Long.parseLong(tokens[0]);
            ride.consumption = Double.parseDouble(tokens[1]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
