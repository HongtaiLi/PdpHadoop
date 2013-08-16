package pdp.dbconverter.mapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import pdp.record.PdpRecord;

public class DBMapper extends Mapper<LongWritable,PdpRecord,Text,DoubleWritable>{

	//private String table;
	@Override
	protected void setup(Context context){
		//table = context.getConfiguration().get("tbl_name");
	}
	
	@Override
	public void map(LongWritable offset,PdpRecord pdpRecord,Context context){
		try {
		//	String grade = brRecord.GRADE;
			String char_id = pdpRecord.CHAR_ID;
			String val_tbl = pdpRecord.VALUE_TBL;
			String resv = pdpRecord.RESV_FIELD4;
			
			context.write(new Text(char_id+":val"),new DoubleWritable(new Double(val_tbl)));
			context.write(new Text(char_id+":resv"),new DoubleWritable(new Double(resv)));
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
