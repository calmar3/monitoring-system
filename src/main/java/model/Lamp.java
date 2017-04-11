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
    private String address;
    private long timestamp;
    private long lastSubstitutionDate;
    private long residualLifeTime;

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
        sb.append(this.lampId).append(",");
        sb.append(this.consumption).append(",");
        sb.append(this.address);
        sb.append(" timestamp : ").append(this.timestamp);

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
