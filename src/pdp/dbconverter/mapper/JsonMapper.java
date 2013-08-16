package pdp.dbconverter.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pdp.record.CpdpRecord;
import pdp.util.JsonRecord;

import com.google.gson.Gson;

public class JsonMapper extends Mapper<LongWritable,CpdpRecord,Text,Text>{

	// private String table;
	@Override
	protected void setup(Context context) {
		// table = context.getConfiguration().get("tbl_name");
	}

	@Override
	public void map(LongWritable offset, CpdpRecord pdpRecord, Context context) {
		
		try {
			String sublot_id = pdpRecord.SUBLOT_ID;
			String grade = pdpRecord.FLOW_GRADE;
			String char_id = pdpRecord.CHAR_ID;
			String val_tbl = pdpRecord.VALUE_TBL;
			String resv = pdpRecord.RESV_FIELD4;
			Text key = new Text(sublot_id);
			
			Gson gson = new Gson();
			JsonRecord record = new JsonRecord(grade, char_id, val_tbl, resv);
			String value  = gson.toJson(record);
			context.write(key, new Text(value));

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
}
