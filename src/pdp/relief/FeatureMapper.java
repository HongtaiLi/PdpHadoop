package pdp.relief;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import pdp.util.GradeKeyPair;

import com.google.gson.Gson;

public class FeatureMapper extends
		Mapper<LongWritable, Text, Text, Text> {
		Gson gson = new Gson();
	public void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {
		String [] kv = line.toString().split("\\s");
		
		ReliefType rtype = gson.fromJson(kv[1], ReliefType.class);
		GradeKeyPair gradeKeyPair = gson.fromJson(kv[0], GradeKeyPair.class);
		
		String sublot_id = gradeKeyPair.getKey();
		
		Map <String,Double> fMap = rtype.getFeatureMap();
		boolean isHit = rtype.getIsHit();
		
		Iterator it = fMap.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry pairs = (Map.Entry)it.next();
			String char_id = (String)pairs.getKey();
			Double val = (Double)pairs.getValue();
			//if(isHit)val*=-1;
			context.write(new Text(char_id), new Text(val+":"+isHit+":"+sublot_id));
		}
	}
}
