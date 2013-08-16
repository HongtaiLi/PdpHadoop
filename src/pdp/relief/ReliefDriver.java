package pdp.relief;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

public class ReliefDriver {

	/**
	 * @param args
	 */
	public static final String NAME = "PdpHadoop";
	private static long numberItems;
	private static String hdfsRoot = "pdp/json/";
	private static Path samplePath;
	private static Path rPath;

	// public static HTable htable;

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("t", "table", true,
				"table(DB or HDFS) to read from (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("o", "output", true, "the output for result");
		o.setArgName("output-name");
		o.setRequired(false);
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

	public static void sampleJob(Configuration conf, String table)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Relief Sample");
		job.setJarByClass(ReliefDriver.class);
		job.setMapperClass(SampleMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setNumReduceTasks(100);
		String[] tbls = table.split(",");
		for (String tbl : tbls) {
			FileInputFormat.addInputPath(job, new Path(hdfsRoot + tbl));
		}
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileOutputFormat
				.setOutputPath(job, new Path(hdfsRoot + "ReliefSample"));
		
		job.waitForCompletion(true);
		samplePath = FileOutputFormat.getOutputPath(job);
		numberItems = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue();

	}

	public static void reliefJob(Configuration conf,String table)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Job job = new Job(conf, "Relief");
		job.setJarByClass(ReliefDriver.class);
		job.setMapperClass(ReliefMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.getConfiguration().set("numberItems", String.valueOf(numberItems));
		String[] tbls = table.split(",");
		for (String tbl : tbls) {
			FileInputFormat.addInputPath(job, new Path(hdfsRoot + tbl));
		}
		DistributedCache.addCacheFile(new URI(samplePath.toString()+"/part-r-00000"), job.getConfiguration());
		System.out.println("SamplePath:"+samplePath.toString());
		job.setCombinerClass(ReliefReducer.class);
		job.setReducerClass(ReliefReducer.class);
		job.getConfiguration().set("sampleURI", samplePath.toUri().toString());
		job.setNumReduceTasks(100);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(hdfsRoot + "relief"));
		job.waitForCompletion(true);
		rPath = FileOutputFormat.getOutputPath(job);
		
	}
	public static void featureJob(Configuration conf,String output) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job(conf, "Relief Feature");
		job.setJarByClass(ReliefDriver.class);
		job.setMapperClass(FeatureMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, rPath);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FeatureReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Writable.class);
		FileOutputFormat.setOutputPath(job, new Path(hdfsRoot + output));
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

		sampleJob(conf, table);
		reliefJob(conf, table);
		featureJob(conf,output);
	}
}
