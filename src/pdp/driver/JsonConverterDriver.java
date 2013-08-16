package pdp.driver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

import pdp.dbconverter.mapper.DBMapper;
import pdp.dbconverter.mapper.JsonMapper;
import pdp.dbconverter.reducer.DBReducer;
import pdp.record.CpdpRecord;
import pdp.record.PdpRecord;
import pdp.record.ResultRecord;
import pdp.statistic.mapper.StatMapper;
import pdp.statistic.reducer.StatReducer;

public class JsonConverterDriver {

	/**
	 * @param args
	 */
	public static final String NAME = "PdpHadoop";

	// public static HTable htable;

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("t", "table", true,
				"table(DB or HDFS) to read from (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("o", "output", true,
				"the database table name for result");
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

	/**
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */

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
		String outputTable = cmd.getOptionValue("o");
		String url = cmd.getOptionValue("url");
		String user = cmd.getOptionValue("u");
		String pass = cmd.getOptionValue("p");

		String[] tableList = table.split(",");

		String hdfsPath = "pdp/json/";
		// Convert and extract DB feilds to key value text
		for (String dbTable : tableList) {
		    System.out.println(dbTable);
			String outCur = hdfsPath + dbTable;
			Job job = new Job(conf, "Json" + dbTable);
			job.setJarByClass(JsonConverterDriver.class);
			job.setMapperClass(JsonMapper.class);
			job.setInputFormatClass(DataDrivenDBInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(DBReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setNumReduceTasks(10);
			FileOutputFormat.setOutputPath(job, new Path(outCur));
			DBConfiguration.configureDB(job.getConfiguration(),
					"com.mysql.jdbc.Driver", url, user, pass);

			String[] fields = {"SUBLOT_ID","FlOW_GRADE","SLOT_NO", "CHAR_ID", "VALUE_TBL",
					"RESV_FIELD4" };

			DataDrivenDBInputFormat.setInput(job, CpdpRecord.class, dbTable,
					null, "SLOT_NO", fields);

			job.waitForCompletion(true);

		}

	}
}
