package model;

/**
 * Created by marco on 27/03/17.
 */
public class Street {

    private String id;
    private Double consumption;

    public Street(String id, Double consumption){
        this.id = id;
        this.consumption = consumption;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.id).append(",");
        this.consumption = Math.round(this.consumption*100.0)/100.0;
        sb.append(this.consumption);
        return sb.toString();
    }
}
