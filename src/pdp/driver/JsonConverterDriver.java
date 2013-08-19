package pdp.driver;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

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
//		Option o = new Option("t", "table", true,
//				"table(DB or HDFS) to read from (must exist)");
//		o.setArgName("table-name");
//		o.setRequired(true);
//		options.addOption(o);
//
//		o = new Option("o", "output", true,
//				"the database table name for result");
//		o.setArgName("output-name");
//		o.setRequired(false);
//		options.addOption(o);
//
//		o = new Option("url", "url", true, "address of database");
//		o.setArgName("db-address");
//		o.setRequired(false);
//		options.addOption(o);
//
//		o = new Option("u", "user", true, "username");
//		o.setArgName("user-name");
//		o.setRequired(false);
//		options.addOption(o);
//
//		o = new Option("p", "password", true, "password of database");
//		o.setArgName("password");
//		o.setRequired(false);
//		options.addOption(o);
		
		Option o = new Option("date","date",true,"date format yyyymm");
		o.setArgName("date-name");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("f","flow",true,"flow Name");
		o.setArgName("flow-name");
		o.setRequired(true);
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

	private static ArrayList <String> getTables(Connection conn,String flow,String yyyymm){
		
		//select tabname from PDP_FLOW_MONTH where flow='BR' and yyyymmdd like "201305%" and LENGTH(yyyymmdd)=8;
		String sql = "select tabname from PDP_FLOW_MONTH where flow=? and yyyymmdd like\""+yyyymm+"%\""+"and LENGTH(yyyymmdd)=8";
		ArrayList <String> resList = new ArrayList <String>();
		
		try {
			PreparedStatement pst =  conn.prepareStatement(sql);
			pst.setString(1, flow);
			
			ResultSet rs =  pst.executeQuery();
		
			while(rs.next()){
				resList.add(rs.getString("tabname"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resList;
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

		String configFileDir = System.getenv("PDP_HOME");
		String hadoopHost = null;
		String hadoopPort = null;
		String dbHost = null;
		String dbPort = null;
		String dbName = null;
		String dbUser = null;
		String dbPass = null;
		String lsdaDbHost = null;
		String lsdaDbName = null;
		String lsdaDbPort = null;
		String lsdaDbUser = null;
		String lsdaDbPass = null;
		
		if (configFileDir != null) {
			Properties prop = new Properties();
			String propFileName = "conf/lsda.conf";

			prop.load(new FileReader(configFileDir + "/" + propFileName));
			hadoopHost = prop.getProperty("lsda.hadoop.host");
			hadoopPort = prop.getProperty("lsda.hadoop.port");
			dbHost = prop.getProperty("lsda.pdp.db.host");
			dbName = prop.getProperty("lsda.pdp.db.name");
			dbPort = prop.getProperty("lsda.pdp.db.port");
			dbUser = prop.getProperty("lsda.pdp.db.user");
			dbPass = prop.getProperty("lsda.pdp.db.pass");
			
			lsdaDbHost = prop.getProperty("lsda.db.host");
			lsdaDbName = prop.getProperty("lsda.db.name");
			lsdaDbPort = prop.getProperty("lsda.db.port");
			lsdaDbUser = prop.getProperty("lsda.db.user");
			lsdaDbPass = prop.getProperty("lsda.db.pass");
		}
		
		
	
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop,supergroup");
		conf.set("mapred.job.tracker", hadoopHost+":"+hadoopPort);
		conf.set("dfs.socket.timeout", "1210000");
		conf.set("mapred.map.tasks", "20");
		
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		// check debug flag and other options
		if (cmd.hasOption("d"))
			conf.set("conf.debug", "true");
		// get details
		//String table = cmd.getOptionValue("t");
		//String outputTable = cmd.getOptionValue("o");
		
		String date = cmd.getOptionValue("date");
	//	String outputTable = cmd.getOptionValue("o");
		String flowName = cmd.getOptionValue("f");
		String pdpUrl = "jdbc:mysql://"+dbHost+":"+dbPort+"/"+dbName;
		String lsdaUrl = "jdbc:mysql://"+lsdaDbHost+":"+lsdaDbPort+"/"+lsdaDbName;
	    
		Connection lsdaConn = DriverManager.getConnection(lsdaUrl, lsdaDbUser, lsdaDbPass);

		ArrayList <String> tableList =  getTables(lsdaConn,flowName,date);
		
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
			FileSystem fs = FileSystem.get(job.getConfiguration()); 
			Path outputPath = new Path(outCur);
			if(fs.exists(outputPath)){
				fs.delete(outputPath,true);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			DBConfiguration.configureDB(job.getConfiguration(),
					"com.mysql.jdbc.Driver", pdpUrl, dbUser, dbPass);

			String[] fields = {"SUBLOT_ID","FlOW_GRADE","SLOT_NO", "CHAR_ID", "VALUE_TBL",
					"RESV_FIELD4" };
			
			
			DataDrivenDBInputFormat.setInput(job, CpdpRecord.class, dbTable,
					null, "SLOT_NO", fields);

			job.waitForCompletion(true);

		}

	}
}
