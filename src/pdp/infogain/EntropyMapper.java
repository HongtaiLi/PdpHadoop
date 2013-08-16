package pdp.infogain;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import pdp.util.GradeCount;
import pdp.util.IdValuePair;

import com.google.gson.Gson;

public class EntropyMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Integer numberItems;
	Gson gson = new Gson();

	protected void setup(Context context){
		numberItems  = new Integer(context.getConfiguration().get("numberItems"));
	}

	public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
		String [] vals = line.toString().split("\\s");
		IdValuePair id_value = gson.fromJson(vals[0],IdValuePair.class);
  //	GradeCount gradecount = gson.fromJson(vals[1], GradeCount.class);
//		Map <String,Integer> labelCount = gradecount.getLabelCount();
//		Double total = Double.valueOf(gradecount.getTotal());
//		
//		Iterator it = labelCount.entrySet().iterator();
//		double entropy = 0.0;
//		double LOG2 = Math.log(2.0);
//		while(it.hasNext()){
//			Map.Entry pairs = (Map.Entry)it.next();
//			Double count =new Double((Integer)pairs.getValue()) ;
//			entropy += -1.0*(count/total)*(Math.log(count/total)/LOG2);
//		}
//	//	entropy *= (total/numberItems);
//		System.out.println("tt"+numberItems);
		if(!id_value.getVal().trim().isEmpty()){
			context.write(new Text(id_value.getChar_id()), new Text(vals[1]));
		}
	}
}
