package pdp.util;

public class JsonRecord {
	private String grade;
	private String char_id;
	private String val;
	
	public JsonRecord(String grade, String char_id, String val, String resv) {
		super();
		this.grade = grade;
		this.char_id = char_id;
		this.val = val;
		this.resv = resv;
	}

	private String resv;
	
	public JsonRecord(){}

	public String getGrade() {
		return grade;
	}

	public String getChar_id() {
		return char_id;
	}

	public String getVal() {
		return val;
	}

	public String getResv() {
		return resv;
	}
	
	
}
