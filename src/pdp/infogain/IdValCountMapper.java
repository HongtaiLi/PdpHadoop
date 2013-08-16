package pdp.infogain;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

import pdp.util.IdValuePair;
import pdp.util.JsonResult;

import com.google.gson.Gson;

public class IdValCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	Gson gson = new Gson();
	Map<String, ArrayList<String>> valMap;

	public void map(LongWritable offset, Text line, Context context) {
		
		JsonResult result = gson.fromJson(line.toString(), JsonResult.class);
		valMap = result.getValMap();
		String grade = result.getGrade();

		Iterator it = valMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String char_id = (String) pairs.getKey();

			ArrayList<String> vals = (ArrayList<String>) pairs.getValue();
			for (String val : vals) {
				if (val.trim().isEmpty()) {
					continue;
				}
				IdValuePair keyPair = new IdValuePair(char_id, val);
				String key = gson.toJson(keyPair);
				try {
					context.write(new Text(key), new Text(grade));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
