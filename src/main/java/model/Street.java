package model;

/**
 * Created by marco on 27/03/17.
 */
public class Street {

    private String id;
    private Double consumption;
    private long timestamp;

    public Street(String id, Double consumption, long timestamp){
        this.id = id;
        this.consumption = consumption;
        this.timestamp = timestamp;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getConsumption() {
        return consumption;
    }

    public void setConsumption(Double consumption) {
        this.consumption = consumption;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.id).append(",");
        this.consumption = Math.round(this.consumption*100.0)/100.0;
        sb.append(this.consumption).append(",");
        sb.append(" timestamp : ").append(this.timestamp);
        return sb.toString();
    }
}
