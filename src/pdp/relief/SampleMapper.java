package pdp.relief;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import pdp.record.CpdpRecord;
import pdp.util.JsonResult;

import com.google.gson.Gson;

public class SampleMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	private long len;

	/*
	 * protected void setup(Context context) throws IOException,
	 * InterruptedException { len = context.getInputSplit().getLength(); }
	 */
	Gson gson = new Gson();
	Random rdn = new Random();

	@Override
	public void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {
		JsonResult record = gson.fromJson(line.toString(), JsonResult.class);
		// if (record.getGrade().equals("S")) {
		// context.write(line,NullWritable.get());
		// } else {
		int p = 5;
		int num = rdn.nextInt(p);
		if (num == p / 2) {
			context.write(line, NullWritable.get());
		}
		// }

	}
}
