package pdp.other;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

import pdp.util.IdValuePair;
import pdp.util.JsonResult;

import com.google.gson.Gson;

public class CountRepeatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Gson gson = new Gson();
	Map<String, ArrayList<String>> valMap;

	public void map(LongWritable offset, Text line, Context context) {
		JsonResult result = gson.fromJson(line.toString(), JsonResult.class);
		valMap = result.getValMap();
		int normal = 0;
		int unnormal = 0;
		Iterator it = valMap.entrySet().iterator();
		boolean flag = false;
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String char_id = (String) pairs.getKey();

			ArrayList<String> vals = (ArrayList<String>) pairs.getValue();
			if(vals.size()>1){
				flag = true;
				unnormal++;
			}
			else
			{
				normal++;
			}
		}
		try {
			context.write(new Text("unnormal"), new IntWritable(unnormal));
			context.write(new Text("normal"), new IntWritable(normal));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
