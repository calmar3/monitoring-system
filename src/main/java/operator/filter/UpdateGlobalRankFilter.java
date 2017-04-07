package operator.filter;

import model.Lamp;
import model.Ranking;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.TreeSet;

public final class UpdateGlobalRankFilter implements FilterFunction<TreeSet<Lamp>> {
	
	private static final long serialVersionUID = 1L;
	
	private Ranking lastRank;
	
	public UpdateGlobalRankFilter(final int rankMaxSize) {
		this.lastRank = new Ranking(rankMaxSize);
	}

	@Override
	public boolean filter(TreeSet<Lamp> newRank) throws Exception {
		if(lastRank.getLampRank().size() == 0){
			this.lastRank.setLampRank(newRank);
			return true;
		}
		else if (this.lastRank.isEquivalent(newRank, this.lastRank.getLampRank())) {
			return false;
		} else {			
			this.lastRank.setLampRank(newRank);
			return true;
		}
	}
	
}
