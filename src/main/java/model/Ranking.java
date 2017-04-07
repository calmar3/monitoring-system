package model;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

public class Ranking implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int rankMaxSize;
	private TreeSet<Lamp> lampRank;
	
	public Ranking(){}
	
	public Ranking(int rankMaxSize){
		this.rankMaxSize = rankMaxSize;
		this.lampRank = new TreeSet<Lamp>(new ComparatorLamp());
	}

	public int getRankMaxSize() {
		return rankMaxSize;
	}
	
	public void setRankMaxSize(int rankMaxSize) {
		this.rankMaxSize = rankMaxSize;
	}
	
	public TreeSet<Lamp> getLampRank() {
		return lampRank;
	}
	
	public void setLampRank(TreeSet<Lamp> lampRank) {
		this.lampRank = lampRank;
	}
	
	public Lamp findLamp(Lamp lamp){
		for(Lamp tmp : this.lampRank){
			if(tmp.getLampId() == lamp.getLampId())
				return tmp;
		}
		return null;
	}
	
	public boolean overMaxSize(){
		if(this.lampRank.size() > this.rankMaxSize)
			return true;
		else 
			return false;
	}
	
	public boolean isEquivalent(TreeSet<Lamp> newRank, TreeSet<Lamp> lastRank){
		if(newRank.size() != lastRank.size()){
			return false;
		}
		else {
			Iterator<Lamp> iterator = lastRank.iterator();
			for(Lamp newRankLamp : newRank){
				Lamp oldRankLamp = iterator.next();
				if(newRankLamp.getLampId() != oldRankLamp.getLampId())
					return false;
			}
			return true;
		}
	}

}
