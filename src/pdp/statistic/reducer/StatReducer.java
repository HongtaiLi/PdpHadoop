package pdp.statistic.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pdp.record.ResultRecord;

public class StatReducer extends
		Reducer<Text, Text, ResultRecord, NullWritable> {

	DescriptiveStatistics val_stats = new DescriptiveStatistics();
	DescriptiveStatistics resv_stats = new DescriptiveStatistics();
	Map<Double, Integer> val_mMap = new TreeMap<Double, Integer>();
	Map<Double, Integer> resv_mMap = new TreeMap<Double, Integer>();

	public void reduce(Text key, Iterable<Text> values, Context context) {
		val_stats.clear();
		resv_stats.clear();
		val_mMap.clear();
		resv_mMap.clear();
		
		for (Text value : values) {
			String[] vals = value.toString().split(":");
			if(vals[1].trim().isEmpty()){
				continue;
			}
			Double val = new Double(vals[1]);
			if (vals[0].equals("val")) {
				val_stats.addValue(val);
				if (val_mMap.containsKey(val)) {
					int cur = val_mMap.get(val);
					val_mMap.put(val, cur + 1);
				} else {
					val_mMap.put(val, 1);
				}
			} else if (vals[0].equals("resv")){
				resv_stats.addValue(val);
				if (resv_mMap.containsKey(val)) {
					int cur = resv_mMap.get(val);
					resv_mMap.put(val, cur + 1);
				} else {
					resv_mMap.put(val, 1);
				}
			}

		}
		/*
		 * Count value count and distribution
		 */
		Iterator it = val_mMap.entrySet().iterator();
		Integer count = 0;
		String distri = "";
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			Double val = (Double) entry.getKey();
			Integer val_cnt = (Integer) entry.getValue();
			count += val_cnt;
			if (distri == "") {
				distri += (val + ":" + val_cnt);
			} else {
				distri += ("," + val + ":" + val_cnt);
			}
		}
		/*
		 * Count resv count and distribution
		 * 
		 */
		Iterator resv_it = resv_mMap.entrySet().iterator();
		Integer resv_count = 0;
		String resv_distri = "";
		while (resv_it.hasNext()) {
			Map.Entry entry = (Map.Entry) resv_it.next();
			Double val = (Double) entry.getKey();
			Integer val_cnt = (Integer) entry.getValue();
			resv_count += val_cnt;
			if (resv_distri == "") {
				resv_distri += (val + ":" + val_cnt);
			} else {
				resv_distri += ("," + val + ":" + val_cnt);
			}
		}

		Double mean = val_stats.getMean();
		Double resv_mean = resv_stats.getMean();
		Double median = val_stats.getPercentile(50);
		Double resv_median = resv_stats.getPercentile(50);
		Double std = val_stats.getStandardDeviation();
		Double resv_std = resv_stats.getStandardDeviation();
		Double max = val_stats.getMax();
		Double resv_max = resv_stats.getMax();
		Double min = val_stats.getMin();
		Double resv_min = resv_stats.getMin();
		Double kurtosis = val_stats.getKurtosis();
		Double resv_kurtosis = resv_stats.getKurtosis();
		Double skewness = val_stats.getSkewness();
		Double resv_skewness = resv_stats.getSkewness();


		// String parten = "#.##";
		// DecimalFormat decimal = new DecimalFormat(parten);
	    DecimalFormat twoDForm = new DecimalFormat("#.##");

	    if(!Double.isNaN(mean))
	    {
	    	mean = Double.valueOf(twoDForm.format(mean));
	    }
	    if(!Double.isNaN(resv_mean)){
	    	resv_mean = Double.valueOf(twoDForm.format(resv_mean));
	    }
		
		if(!Double.isNaN(std)){
			std = Double.valueOf(twoDForm.format(std));
		}
		if(!Double.isNaN(resv_std)){
			resv_std = Double.valueOf(twoDForm.format(resv_std));

		}

		if (!Double.isNaN(kurtosis)) {
			kurtosis = Double.valueOf(twoDForm.format(kurtosis));
		}
		if(!Double.isNaN(resv_kurtosis)){
			resv_kurtosis = Double.valueOf(twoDForm.format(resv_kurtosis));
		}

		if (!Double.isNaN(skewness)) {
			skewness = Double.valueOf(twoDForm.format(skewness));
		}
		if(!Double.isNaN(resv_skewness)){
			resv_skewness = Double.valueOf(twoDForm.format(resv_skewness));
		}

		try {
			ResultRecord resultRecord = new ResultRecord();
			resultRecord.CHAR_ID = key.toString();
			resultRecord.COUNT = count.toString();
			resultRecord.DISTINCTVALUES =  new Integer(val_mMap.keySet().size()).toString();
			resultRecord.STD = std.toString();
			resultRecord.MEDIAN = median.toString();
			resultRecord.MEAN = mean.toString();
			resultRecord.MAX = max.toString();
			resultRecord.MIN = min.toString();
			resultRecord.DISTRIBUTION = distri;
			resultRecord.KURTOSIS = kurtosis.toString();
			resultRecord.SKEWNESS = skewness.toString();
			
			resultRecord.RESV_COUNT = resv_count.toString();
			resultRecord.RESV_DISTINCTVALUES = new Integer(resv_mMap.keySet().size()).toString();
			resultRecord.RESV_STD = resv_std.toString();
			resultRecord.RESV_MEDIAN = resv_median.toString();
			resultRecord.RESV_MEAN = resv_mean.toString();
			resultRecord.RESV_MAX = resv_max.toString();
			resultRecord.RESV_MIN = resv_min.toString();
			resultRecord.RESV_DISTRIBUTION = resv_distri;
			resultRecord.RESV_KURTOSIS = resv_kurtosis.toString();
			resultRecord.RESV_SKEWNESS = resv_skewness.toString();
			context.write(resultRecord, NullWritable.get());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
