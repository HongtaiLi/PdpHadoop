package pdp.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class CpdpRecord implements Writable, DBWritable {

	// String SUBLOT_ID;
	// int SLOT_NO;
	// String GRADE;
	public String SUBLOT_ID;
	public String FLOW_GRADE;
	public int SLOT_NO;
	//public int S
	public String CHAR_ID;
	public String VALUE_TBL;
	public String RESV_FIELD4;
	

	// String EQUIP_ID;

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODOAuto-generated method stub
		this.SUBLOT_ID = Text.readString(in);
		this.SLOT_NO = in.readInt();
		this.FLOW_GRADE = Text.readString(in);
		this.CHAR_ID = Text.readString(in);
		this.VALUE_TBL = Text.readString(in);
		this.RESV_FIELD4 = Text.readString(in);

		// this.EQUIP_ID = Text.readString(in);
		// 

		Random rd = new Random();
		this.SLOT_NO = rd.nextInt(100);
		// this.SLOT_NO *= (int)Math.pow(10, p);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generatedmethod stub
		Text.writeString(out, this.SUBLOT_ID);
		out.writeInt(this.SLOT_NO);
		Text.writeString(out, this.FLOW_GRADE);
		Text.writeString(out, this.CHAR_ID);
		Text.writeString(out, this.VALUE_TBL);
		Text.writeString(out, this.RESV_FIELD4);
		// Text.writeString(out, this.EQUIP_ID);
		
	}

	@Override
	public void readFields(ResultSet result) throws SQLException {
		// TODOAuto-generated method stub
		this.SUBLOT_ID = result.getString("SUBLOT_ID");
		this.SLOT_NO = result.getInt("SLOT_NO");
		this.FLOW_GRADE = result.getString("FLOW_GRADE");
		this.CHAR_ID = result.getString("CHAR_ID");
		this.VALUE_TBL = result.getString("VALUE_TBL");
		this.RESV_FIELD4 = result.getString("RESV_FIELD4");
	    
		
		// this.EQUIP_ID = result.getString("EQUIP_ID");
		 Random rd = new Random();
		 this.SLOT_NO = rd.nextInt(100);
		// this.SLOT_NO *= (int)Math.pow(10, p);
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		// TODO Auto-generatedmethod stub
		stmt.setString(1, this.SUBLOT_ID);
		stmt.setString(4, this.FLOW_GRADE);
		stmt.setInt(3, this.SLOT_NO);
		stmt.setString(11, this.CHAR_ID);
		stmt.setString(12, this.VALUE_TBL);
		stmt.setString(14, this.RESV_FIELD4);
		
		// stmt.setString(9, this.EQUIP_ID);
	}

	@Override
	public String toString() {
		return this.CHAR_ID;
		// TODOAuto-generated method stub
		// return new String(this.SUBLOT_ID+ " " + this.GRADE
		// + " " + this.CHAR_ID+" "+this.VALUE_TBL+" "+this.RESV_FIELD4);
	}
}
