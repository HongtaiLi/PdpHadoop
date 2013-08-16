package pdp.relief;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//Some problem with this algorithm, because the update seems unfair
//I need to know where the char_id's sublot_id,common set of hit and 
//missing of char_id

public class FeatureReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	Map<String, Double> hitMap = new HashMap<String, Double>();
	Map<String, Double> missMap = new HashMap<String, Double>();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		hitMap.clear();
		missMap.clear();
		double sum = 0.0;
		// double h_cnt = 0.0;
		// double m_cnt = 0.0;
		// double h_sum = 0.0;
		// double m_sum = 0.0;
		double cnt = 0.0;

		for (Text val : values) {
			String vals[] = val.toString().split(":");
			if (vals[1].equals("true")) {
				// h_cnt += 1.0;
				// h_sum += (new Double(vals[0]) * -1);
				hitMap.put(vals[2], new Double(vals[0]) * -1);
			} else {
				missMap.put(vals[2], new Double(vals[0]));
				// m_cnt += 1.0;
				// m_sum += (new Double(vals[0]));
			}

		}

		Iterator it = hitMap.entrySet().iterator();
		boolean flag = false;
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String sublot_id = (String) pairs.getKey();
			if (missMap.containsKey(sublot_id)) {
				sum += ((Double) pairs.getValue() + missMap.get(sublot_id));
				cnt += 1.0;
				flag = true;
			}

		}
		System.out.println("char_id:" + key.toString() + " sum" + sum + "cnt"
				+ cnt);

		if (flag) {
			context.write(key, new DoubleWritable(sum / cnt));
		}
	}

}
