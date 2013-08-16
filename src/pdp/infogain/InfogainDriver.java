package pdp.infogain;

import java.io.IOException;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class InfogainDriver {

	/**
	 * @param args
	 */
	public static final String NAME = "PdpHadoop";
	private static long numberItems;
	private static String hdfsRoot = "";
	private static Path countPath;

	// public static HTable htable;

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("t", "table", true,
				"table(DB or HDFS) to read from (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("o", "output", true,
				"the output for result");
		o.setArgName("output-name");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("url", "url", true, "address of database");
		o.setArgName("db-address");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("u", "user", true, "username");
		o.setArgName("user-name");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("p", "password", true, "password of database");
		o.setArgName("password");
		o.setRequired(false);
		options.addOption(o);

		options.addOption("d", "debug", false, "switch on DEBUG log level");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		if (cmd.hasOption("d")) {
			Logger log = Logger.getLogger("mapreduce");
			log.setLevel(Level.DEBUG);
			System.out.println("DEBUG ON");
		}
		return cmd;
	}

	public static void countJob(Configuration conf,String table) throws IOException, InterruptedException, ClassNotFoundException {		
		Job job = new Job(conf,"Info-gain count");
		job.setJarByClass(InfogainDriver.class);
		job.setMapperClass(IdValCountMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		String []tbls = table.split(",");
		for(String tbl:tbls){
			FileInputFormat.addInputPath(job, new Path(hdfsRoot + tbl));
		}
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(IdValCountReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(hdfsRoot+"countJob"));
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		countPath = FileOutputFormat.getOutputPath(job);
		job.waitForCompletion(true);

		numberItems = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();
		
	}

	public static void conditionalEntropy(Configuration conf,String output) throws IOException, InterruptedException, ClassNotFoundException {
//		String fileName = "part-r-0000";
		Job job = new Job(conf,"Info-gain");
		job.setJarByClass(InfogainDriver.class);
		job.setMapperClass(EntropyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, countPath);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(EntropyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(20);
		FileOutputFormat.setOutputPath(job, new Path(hdfsRoot+output));
		job.getConfiguration().set("numberItems", String.valueOf(numberItems));
		System.out.println(numberItems);
		job.waitForCompletion(true);

	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop,supergroup");
		conf.set("mapred.job.tracker", "datamining-node01.cs.fiu.edu:30001");
		conf.set("dfs.socket.timeout", "1210000");

		conf.set("mapred.map.tasks", "100");

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		// check debug flag and other options
		if (cmd.hasOption("d"))
			conf.set("conf.debug", "true");
		// get details
		String table = cmd.getOptionValue("t");
		String output = cmd.getOptionValue("o");
		String url = cmd.getOptionValue("url");
		String user = cmd.getOptionValue("u");
		String pass = cmd.getOptionValue("p");

		countJob(conf,table);
		conditionalEntropy(conf,output);

	}
}
