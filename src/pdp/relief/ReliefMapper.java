package pdp.relief;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import pdp.util.GradeKeyPair;
import pdp.util.JsonResult;

import com.google.gson.Gson;

public class ReliefMapper extends Mapper<LongWritable, Text, Text, Text> {

	// private Map<String,ReliefType> hitMap = new HashMap<String,
	// ReliefType>();
	// private Map<String,ReliefType> missMap = new HashMap<String,
	// ReliefType>() ;
	// private Map <String,Double> curfeatureMap = new HashMap<String,
	// Double>();
	Path[] localFiles;
	BufferedReader br;

	Gson gson = new Gson();

	private String m;

	protected void setup(Context context) throws IOException {
		// String sampleURI = context.getConfiguration().get("sampleURI");
		DistributedCache.getCacheFiles(context.getConfiguration());
		localFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		FileReader fr = new FileReader(localFiles[0].toString());
		br = new BufferedReader(fr);
		m = context.getConfiguration().get("numberItems");
	}

	private double distance(JsonResult r1, JsonResult r2,
			Map<String, Double> curFeatureMap) {
		curFeatureMap.clear();
		Map<String, ArrayList<String>> r1Map = r1.getValMap();
		Map<String, ArrayList<String>> r2Map = r2.getValMap();

		Map<String, ArrayList<String>> bMap = null;
		Map<String, ArrayList<String>> sMap = null;

		// double count = 0.0;
		// Choose bigger one as outer loop
		if (r1Map.size() > r2Map.size()) {
			bMap = r1Map;
			sMap = r2Map;
		} else {
			bMap = r2Map;
			sMap = r1Map;
		}

		double intersection = 0.0;
		double tot = 0.0;
		Iterator it = bMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String char_id = (String) pairs.getKey();
			double diff = 1.0;
			if (sMap.containsKey(char_id)) {
				tot += 1.0;
				ArrayList<String> val2List = sMap.get(char_id);
				ArrayList<String> val1List = (ArrayList<String>) pairs
						.getValue();
				int len1 = val1List.size();
				int len2 = val2List.size();
				if (!(val1List.get(len1 - 1).trim().isEmpty() || val2List
						.get(len2 - 1).trim().isEmpty())) {
					if (val1List.get(len1 - 1).equals(val2List.get(len2 - 1))) {
						intersection += 1.0;
						diff = 0;
					}
					curFeatureMap.put(char_id, diff);
				}

			}
			// Change this to ignore missing value

		}
		return (1.0 - intersection / (2 * tot - intersection)) * (1-tot/(sMap.size()+bMap.size()-tot));
	}

	public void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {
		// hitMap.clear();
		// missMap.clear();

		String shareLine = "";
		JsonResult record = gson.fromJson(line.toString(), JsonResult.class);
		// String re_sublot = record.getSublot_id();
		// String re_label = record.getGrade();
		while ((shareLine = br.readLine()) != null) {
			JsonResult shareRecord = gson.fromJson(shareLine, JsonResult.class);
			// System.out.println("xx"+shareLine);
			if (!shareRecord.getSublot_id().equals(record.getSublot_id())) {
				Map<String, Double> fMap = new HashMap<String, Double>();
				Double dis = distance(shareRecord, record, fMap);
				boolean isHit;
				if (shareRecord.getGrade().equals(record.getGrade())) {
					isHit = true;
				} else {
					isHit = false;
				}
				// if(record.getGrade().equals("N")){
				// System.out.println("NNNN++");
				// throw new IOException();
				//
				// }
				context.write(
						new Text(gson.toJson(new GradeKeyPair(shareRecord
								.getSublot_id(), record.getGrade()))),
						new Text(gson.toJson(new ReliefType(fMap, isHit, dis))));
			}
		}
	}
}
