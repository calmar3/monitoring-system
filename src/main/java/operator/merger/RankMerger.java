package operator.merger;

import model.Lamp;
import model.Ranking;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.TreeSet;

public class RankMerger implements FlatMapFunction <TreeSet<Lamp>, TreeSet<Lamp>>{

	private static final long serialVersionUID = 1L;
	
	private Ranking ranking;
	
	public RankMerger(int maxSizeRank){
		this.ranking = new Ranking(maxSizeRank);
	}
	
	@Override
	public void flatMap(TreeSet<Lamp> partialRank, Collector<TreeSet<Lamp>> globalRank) throws Exception {
		//TODO How to manage deleted Lamp? Maybe black list into the filter...
		if(this.ranking.getLampRank().size() == 0){
			this.ranking.setLampRank(partialRank);																
		}
		else{
			for(Lamp lamp : partialRank){
				if(lamp.getResidualLifeTime() <= this.ranking.getLampRank().last().getResidualLifeTime() && 
				   this.ranking.getLampRank().size() == this.ranking.getRankMaxSize()){
					break;
				}
				Lamp oldLamp = this.ranking.findLamp(lamp);
				if(oldLamp != null){
					if(oldLamp.getResidualLifeTime() >= lamp.getResidualLifeTime() &&
					   oldLamp.getLastSubstitutionDate() <= lamp.getLastSubstitutionDate()){
						continue;
					}
					this.ranking.getLampRank().remove(oldLamp);
				}
				this.ranking.getLampRank().add(lamp);
				if(this.ranking.overMaxSize())
					this.ranking.getLampRank().pollLast();
			}
		}
		
		globalRank.collect(this.ranking.getLampRank());
	}
	
	
}