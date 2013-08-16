package pdp.util;

public class IdValuePair {
	private String char_id;
	private String val;
	public IdValuePair(){}
	
	public IdValuePair(String char_id, String val) {
		super();
		this.char_id = char_id;
		this.val = val;
	}

	public String getChar_id() {
		return char_id;
	}

	public void setChar_id(String char_id) {
		this.char_id = char_id;
	}

	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		this.val = val;
	}
	

}
