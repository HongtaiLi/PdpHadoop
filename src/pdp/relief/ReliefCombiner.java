package pdp.relief;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.Module.SetupContext;


import pdp.util.JsonResult;

import com.google.gson.Gson;

public class ReliefCombiner extends Reducer<Text, Text, Text, Text> {
	Gson gson = new Gson();
	private String m;
	protected void setup(Context context){
	 m =context.getConfiguration().get("numberItems");
	}
	private double distance(JsonResult r1, JsonResult r2,Map <String,Double> curFeatureMap) {
		curFeatureMap.clear();
		Map<String, ArrayList<String>> r1Map = r1.getValMap();
		Map<String, ArrayList<String>> r2Map = r2.getValMap();
		double dis = 0.0;
		Iterator it = r1Map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String char_id = (String) pairs.getKey();
			
			if (r2Map.containsKey(char_id)) {
				ArrayList<String> val2List = r2Map.get(char_id);
				ArrayList <String> val1List = (ArrayList<String>) pairs.getValue();
				if(val1List.get(0).trim().isEmpty()||val2List.get(0).isEmpty()){
					continue;
				}
				else
				{
					double diff = Math.pow(Double.valueOf(val1List.get(0)) - Double.valueOf(val2List.get(0)),2); 
					dis += diff; 
					curFeatureMap.put(char_id,diff/Double.valueOf(m));
				}
				
			}

		}
		return Math.sqrt(dis);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
       // System.out.println("Json+:"+key.toString());
		JsonResult keyResult = null;
		try{
			keyResult = gson.fromJson(key.toString(), JsonResult.class);
		}
		catch (Exception e) {
			// TODO: handle exception
			System.out.println("xx"+key.toString());
		}
		
	    //String sublot_id = keyResult.getSublot_id();
		Map<String,ReliefType> rMap = new HashMap<String,ReliefType>();
	    Map<String,Double> featureMap = new HashMap<String, Double>();
	    
	    String keyGrade = keyResult.getGrade();
	    
		for (Text val : values) {	
			JsonResult valResult = gson.fromJson(val.toString(), JsonResult.class);
			Double dis = distance(keyResult, valResult,featureMap);
			String valGrade = valResult.getGrade();
			
			boolean isHit = false;
			if(valGrade.equals(keyGrade)){
				isHit = true;
			}
			else{
				
				isHit = false;
			}
			
			if(rMap.containsKey(valGrade)){
				 if(dis < rMap.get(valGrade).getDis()){
					rMap.put(valGrade, new ReliefType(new HashMap<String,Double>(featureMap),isHit, dis)); 
				 }
			}
			else
			{
				rMap.put(valGrade, new ReliefType(new HashMap<String,Double>(featureMap),isHit, dis));
			}
		}
		
		context.write(key, new Text(gson.toJson(rMap)));
	}
}
