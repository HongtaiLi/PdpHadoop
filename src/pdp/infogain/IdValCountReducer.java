package pdp.infogain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pdp.util.GradeCount;

import com.google.gson.Gson;


public class IdValCountReducer extends Reducer<Text, Text, Text, Text> {
	private Map<String, Integer> labelCount = new HashMap<String, Integer>();
	Gson gson = new Gson();

	public void reduce(Text key, Iterable<Text> values, Context context) {
		labelCount.clear();
		
		int total = 0;
		for (Text val : values) {
			if(val.toString().trim().isEmpty()){
				continue;
			}
			total += 1;
			if (labelCount.containsKey(val.toString())) {
				Integer cnt = labelCount.get(val.toString());
				labelCount.put(val.toString(), cnt + 1);
			} else {
				labelCount.put(val.toString(), 1);
			}
		}
		
		GradeCount gradecount = new GradeCount(labelCount, total);
		String result = gson.toJson(gradecount);

		try {
			context.write(key, new Text(result));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
