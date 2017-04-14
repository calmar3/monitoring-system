package utils.median;

import com.tdunning.math.stats.ArrayDigest;
import com.tdunning.math.stats.TDigest;

import java.io.Serializable;

public class TDigestMedian implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private ArrayDigest totalDigest;
	
	public TDigestMedian(){
		this.setTotalDigest(TDigest.createArrayDigest(100));
	}

	public ArrayDigest getTotalDigest() {
		return totalDigest;
	}

	public void setTotalDigest(ArrayDigest totalDigest) {
		this.totalDigest = totalDigest;
	}
	
	public void addDigest(double digest){
		this.totalDigest.add(digest);
	}
	
	public double getMedian(){
		return this.totalDigest.quantile(0.5);
	}
}