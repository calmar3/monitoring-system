package model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.io.Serializable;

/**
 * Created by maurizio on 21/03/17.
 */
public class Lamp {

    private long lampId;
    private double consumption;
    private double avgConsumption;
    private double medianConsumption;
    private String address;
    private long timestamp;
    private long lastSubstitutionDate;
    private long residualLifeTime;
    private String city;

    public Lamp() {}

    public Lamp(long lampId, double consumption){
        this.lampId = lampId;
        this.consumption = consumption;
    }

    public Lamp(long lampId, long timestamp, long lastSubstitutionDate) {
        this.lampId = lampId;
        this.timestamp = timestamp;
        this.lastSubstitutionDate = lastSubstitutionDate;
        this.residualLifeTime = timestamp - lastSubstitutionDate;
    }

    public Lamp(long lampId, double consumption,String address){
        this.lampId = lampId;
        this.consumption = consumption;
        this.address = address;
    }

    public Lamp(long lampId, double consumption, String address, long timestamp){
        this.lampId = lampId;
        this.consumption = consumption;
        this.address = address;
        this.timestamp = timestamp;
    }

    public Lamp(long lampId, double consumption, String address, long lastSubstitutionDate, long timestamp){
        this.lampId = lampId;
        this.consumption = consumption;
        this.address = address;
        this.timestamp = timestamp;
        this.lastSubstitutionDate = lastSubstitutionDate;
        this.residualLifeTime = timestamp - lastSubstitutionDate;
    }

    public Lamp(long lampId, double consumption, String city, String address, long timestamp) {
        this.lampId = lampId;
        this.consumption = consumption;
        this.city = city;
        this.address = address;
        this.timestamp = timestamp;
    }

    public Lamp(String city) {
        this.city = city;
    }

    public Lamp(long lampId, double consumption, double avgConsumption, String address, long timestamp) {
        this.lampId = lampId;
        this.consumption = consumption;
        this.avgConsumption = avgConsumption;
        this.address = address;
        this.timestamp = timestamp;
    }

    public Lamp(String city, double consumption) {
        this.city = city;
        this.consumption = consumption;
    }

    public Lamp(int lampId, long timestamp, String address, String city) {
        this.lampId = lampId;
        this.timestamp = timestamp;
        this.address = address;
        this.city = city;
    }

    public Lamp(String city, double consumption, long timestamp) {
        this.city = city;
        this.consumption = consumption;
        this.timestamp = timestamp;
    }

    public long getLampId() {
        return lampId;
    }

    public void setLampId(long lampId) {
        this.lampId = lampId;
    }

    public double getConsumption() {
        return consumption;
    }

    public void setConsumption(double consumption) {
        this.consumption = consumption;
    }

    public double getMedianConsumption() {
        return medianConsumption;
    }

    public void setMedianConsumption(double medianConsumption) {
        this.medianConsumption = medianConsumption;
    }

    public double getAvgConsumption() {
        return avgConsumption;
    }

    public void setAvgConsumption(double avgConsumption) {
        this.avgConsumption = avgConsumption;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getLastSubstitutionDate() {
        return lastSubstitutionDate;
    }

    public void setLastSubstitutionDate(long lastSubstitutionDate) {
        this.lastSubstitutionDate = lastSubstitutionDate;
    }

    public long getResidualLifeTime() {
        return residualLifeTime;
    }

    public void setResidualLifeTime(long residualLifeTime) {
        this.residualLifeTime = residualLifeTime;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.lampId).append(", ");
        sb.append("Cons : ").append(this.consumption).append(", ");
       // sb.append("avgCons : ").append(this.avgConsumption).append(", ");
       // sb.append("medianCons : ").append(this.medianConsumption).append(", ");
        sb.append(this.address).append(", ");
        sb.append("timestamp : ").append(this.timestamp);

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


    public static String toJson(Lamp l) {

        ObjectMapper mapper = new ObjectMapper();

        String jsonInString = new String("");
        try {
            jsonInString = mapper.writeValueAsString(l);
            return jsonInString;

        } catch (JsonProcessingException e) {
            return null;
        }

    }

}
