package pdp.relief;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ReliefReducer extends Reducer<Text, Text, Text, Text> {
	Gson gson = new Gson();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double minDis = Double.MAX_VALUE;
		
		ReliefType minRtype = null;
		for (Text val : values) {
			ReliefType rtype = gson.fromJson(val.toString(), ReliefType.class);
			if (rtype.getDis() < minDis) {
				minDis = rtype.getDis();
				minRtype = rtype;
			}
		}
		
		context.write(key, new Text(gson.toJson(minRtype)));
	}
}
