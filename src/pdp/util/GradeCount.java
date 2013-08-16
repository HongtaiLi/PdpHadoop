package pdp.util;

import java.util.Map;

public class GradeCount {

	private Map <String,Integer> labelCount;
	private int total;
	
	public GradeCount(Map<String, Integer> labelCount, int total) {
		super();
		this.labelCount = labelCount;
		this.total = total;
	}

	public Map<String, Integer> getLabelCount() {
		return labelCount;
	}

	public void setLabelCount(Map<String, Integer> labelCount) {
		this.labelCount = labelCount;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}
	
	
}
