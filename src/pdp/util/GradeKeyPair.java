package pdp.util;

public class GradeKeyPair {
	private String key;
	private String grade;
	
	
	public GradeKeyPair(String key, String grade) {
		super();
		this.key = key;
		this.grade = grade;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getGrade() {
		return grade;
	}
	public void setGrade(String grade) {
		this.grade = grade;
	}
	
}
