package pdp.infogain;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pdp.util.GradeCount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.Gson;

public class EntropyReducer extends
		Reducer<Text, Text, Text, DoubleWritable> {

	Gson gson = new Gson();
	Map<String, Integer> gradeMap = new HashMap<String, Integer>();
	Map<String, Integer> labelCount;
	ArrayList <GradeCount> gradeCntList = new ArrayList<GradeCount>();
	private double LOG2 = Math.log(2.0);

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		gradeMap.clear();
		gradeCntList.clear();
		//double sum = 0.0;
		Integer samp_cnt = 0;
		for (Text val : values) {
			GradeCount gradecount = gson.fromJson(val.toString(),
					GradeCount.class);
			gradeCntList.add(gradecount);
			labelCount = gradecount.getLabelCount();
			samp_cnt += gradecount.getTotal();
			Iterator it = labelCount.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry) it.next();
				String grade = (String) pairs.getKey();
				Integer count = (Integer) pairs.getValue();
				if (gradeMap.containsKey(grade)) {
					Integer preCount = gradeMap.get(grade);
					gradeMap.put(grade, preCount + count);
				} else {
					gradeMap.put(grade, count);
				}

			}
		}

		double entropy = 0.0;

		Iterator it1 = gradeMap.entrySet().iterator();
		while (it1.hasNext()) {
			Map.Entry pairs = (Map.Entry) it1.next();
			Integer t_count = (Integer) pairs.getValue();
			entropy += -1.0
					* (Double.valueOf(t_count) / Double.valueOf(samp_cnt))
					* (Math.log(Double.valueOf(t_count)
							/ Double.valueOf(samp_cnt)) / LOG2);
		}
		double con_entropy = 0.0;
		// calculate conditional entropy
		for (GradeCount gradecount:gradeCntList) {
			labelCount = gradecount.getLabelCount();
			Integer spec_tot = gradecount.getTotal();
			Iterator it = labelCount.entrySet().iterator();
			double spec_entropy = 0.0;
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry) it.next();
				Integer count = (Integer) pairs.getValue();
				spec_entropy += (-1.0
						* (Double.valueOf(count) / Double.valueOf(spec_tot))
						* (Math.log(Double.valueOf(count) / Double
								.valueOf(spec_tot)) / LOG2));
			}
			con_entropy += (spec_entropy * Double.valueOf(spec_tot)
					/ Double.valueOf(samp_cnt));
			//System.out.println("sam_cnt"+samp_cnt);
			//System.out.println("");
		}
		// Info gain
		if(entropy!=0.0)
		{
			//context.write(key, new DoubleWritable((entropy-con_entropy)/entropy));
			context.write(key, new DoubleWritable((entropy-con_entropy)));
		}
	}

}
