package pdp.dbconverter.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;

import pdp.util.JsonRecord;
import pdp.util.JsonResult;

public class DBReducer extends Reducer<Text, Text, Text, NullWritable> {

	private Gson gson = new Gson();
	private Map <String,ArrayList<String>> valMap = new HashMap<String, ArrayList<String>>();
	private Map <String,ArrayList<String>> resvMap = new HashMap<String, ArrayList<String>>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) {
		valMap.clear();
		resvMap.clear();
		JsonResult result = new JsonResult();
		
		for(Text val:values){
			String json = val.toString();
			JsonRecord record = gson.fromJson(json, JsonRecord.class);
			result.setGrade(record.getGrade());
			String char_id = record.getChar_id();
			String value = record.getVal();
			String resv  = record.getResv();
			if(!valMap.containsKey(char_id)){
				ArrayList<String> valList = new ArrayList<String>();
				valList.add(value);
				valMap.put(char_id,valList);
			}
			else
			{
				ArrayList<String> valList;
				valList = valMap.get(char_id);
				valList.add(value);
				valMap.put(char_id, valList);
			}
			
			if(!resvMap.containsKey(char_id))
			{
				ArrayList<String> resvList = new ArrayList<String>();
				resvList.add(resv);
				resvMap.put(char_id, resvList);
			}
			else
			{
				ArrayList <String> resvList = resvMap.get(char_id);
				resvList.add(resv);
				resvMap.put(char_id, resvList);
			}
		}
		result.setSublot_id(key.toString());
		result.setValMap(valMap);
		result.setResvMap(resvMap);
		String output = gson.toJson(result);
		
		try {
			context.write(new Text(output), NullWritable.get());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
