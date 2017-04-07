package model;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorLamp implements Comparator<Lamp>, Serializable{

	private static final long serialVersionUID = 1L;

	public ComparatorLamp(){}
	
	@Override
	public int compare(Lamp l1, Lamp l2) {
		if(l1.getResidualLifeTime() == l2.getResidualLifeTime())
			return 0;
		else if(l1.getResidualLifeTime() > l2.getResidualLifeTime())
			return -1;
		else 
			return 1;
	}

}
