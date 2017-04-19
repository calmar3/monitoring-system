package operator.window.windowfunction;

import model.Lamp;
import model.Ranking;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.TreeSet;

public class LampRankerWF implements WindowFunction <Lamp,TreeSet<Lamp>, Long, TimeWindow>{

	private static final long serialVersionUID = 1L;
	
	private Ranking ranking;
	
	public LampRankerWF(int maxSizeRank){
		this.ranking = new Ranking(maxSizeRank);
	}
	
	@Override
	public void apply(Long key, TimeWindow window, Iterable<Lamp> lampStream, Collector<TreeSet<Lamp>> partialRank) throws Exception {

		for(Lamp lamp : lampStream){
			Lamp oldLamp = this.ranking.findLamp(lamp);

			if(oldLamp != null){
				this.ranking.getLampRank().remove(oldLamp);
			}
			this.ranking.getLampRank().add(lamp);
			if(this.ranking.overMaxSize())
				this.ranking.getLampRank().pollLast();

			partialRank.collect(this.ranking.getLampRank());
		}
	}

}