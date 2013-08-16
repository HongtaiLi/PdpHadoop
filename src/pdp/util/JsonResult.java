package pdp.util;

import java.util.ArrayList;
import java.util.Map;

public class JsonResult {
	private String sublot_id;
	private String grade;
	private Map<String,ArrayList<String>> valMap;
	private Map<String,ArrayList<String>> resvMap;
	
	public JsonResult() {
	}

	public JsonResult(String sublot_id, String grade,
			Map<String, ArrayList<String>> valMap,
			Map<String, ArrayList<String>> resvMap) {
		super();
		this.sublot_id = sublot_id;
		this.grade = grade;
		this.valMap = valMap;
		this.resvMap = resvMap;
	}

	public String getSublot_id() {
		return sublot_id;
	}

	public void setSublot_id(String sublot_id) {
		this.sublot_id = sublot_id;
	}

	public String getGrade() {
		return grade;
	}

	public void setGrade(String grade) {
		this.grade = grade;
	}

	public Map<String, ArrayList<String>> getValMap() {
		return valMap;
	}

	public void setValMap(Map<String, ArrayList<String>> valMap) {
		this.valMap = valMap;
	}

	public Map<String, ArrayList<String>> getResvMap() {
		return resvMap;
	}

	public void setResvMap(Map<String, ArrayList<String>> resvMap) {
		this.resvMap = resvMap;
	}

	

}
