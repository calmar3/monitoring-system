package model;

import com.tdunning.math.stats.ArrayDigest;
import com.tdunning.math.stats.TDigest;

import java.io.Serializable;

//import com.madhukaraphatak.sizeof.SizeEstimator;

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
	
	public void printInfo(){
		System.out.println("\nmedian: "+getMedian()+"\n"+"number of stored elements: "+this.totalDigest.size()+"\ncompression factor: "+this.totalDigest.compression()+"\n");
		System.out.println("\nhow many bytes is needed to serialize t-digest object: "+this.totalDigest.byteSize()+"bytes\n");
		//System.out.println("\nhow much memory is needed for totalDigest object: "+SizeEstimator.estimate(this.totalDigest)+"bytes\n");
	}

}