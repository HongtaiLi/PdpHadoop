package pdp.relief;

import java.util.Map;

public class ReliefType {

	 private Map <String,Double> featureMap;
	private Boolean isHit;
	private double dis;
	//private String record;

	public ReliefType() {

	}

	public ReliefType(Map <String,Double> featureMap,Boolean isHit, double dis) {
		super();
		this.featureMap = featureMap;
		//this.record = record;
		this.isHit = isHit;
		this.dis = dis;
	}

	public Map<String, Double> getFeatureMap() {
		return featureMap;
	}

	public void setFeatureMap(Map<String, Double> featureMap) {
		this.featureMap = featureMap;
	}

	public Boolean getIsHit() {
		return isHit;
	}

	public void setIsHit(Boolean isHit) {
		this.isHit = isHit;
	}

	public double getDis() {
		return dis;
	}

	public void setDis(double dis) {
		this.dis = dis;
	}

//	public String getRecord() {
//		return record;
//	}
//
//	public void setRecord(String record) {
//		this.record = record;
//	}

}
