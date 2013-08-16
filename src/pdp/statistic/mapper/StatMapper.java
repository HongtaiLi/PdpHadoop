package pdp.statistic.mapper;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pdp.util.JsonResult;
//import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

public class StatMapper extends Mapper<Object, Text, Text, Text> {

	Gson gson = new Gson();

	public void setup(Context context) {

	}

	public void map(Object key, Text line, Context context) {
		String parten = "#.####";
		DecimalFormat decimal = new DecimalFormat(parten);
		try {
			// String record = line.toString();
			JsonResult record = gson
					.fromJson(line.toString(), JsonResult.class);
			Map<String, ArrayList<String>> valMap = record.getValMap();
			Map<String, ArrayList<String>> resvMap = record.getResvMap();

			Iterator it = valMap.entrySet().iterator();

			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry) it.next();
				String char_id = (String) pairs.getKey();
				ArrayList<String> valList = (ArrayList<String>) pairs
						.getValue();

				for (String val : valList) {
					if (!val.trim().isEmpty()) {
						context.write(new Text(char_id), new Text("val" + ":"
								+ val));
					}
				}

			}

			it = resvMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry) it.next();
				String char_id = (String) pairs.getKey();
				ArrayList<String> valList = (ArrayList<String>) pairs.getValue();

				for (String val : valList) {
					if (!val.trim().isEmpty()) {
						context.write(new Text(char_id), new Text("resv" + ":"
								+ val));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
