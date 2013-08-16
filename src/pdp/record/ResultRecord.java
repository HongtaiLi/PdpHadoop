package pdp.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ResultRecord implements Writable, DBWritable {

	public String CHAR_ID;

	public String COUNT;
	public String DISTINCTVALUES;
	public String STD;
	public String MEDIAN;
	public String MEAN;
	public String MAX;
	public String MIN;
	public String DISTRIBUTION;
	public String KURTOSIS;
	public String SKEWNESS;

	public String RESV_COUNT;
	public String RESV_DISTINCTVALUES;
	public String RESV_STD;
	public String RESV_MEDIAN;
	public String RESV_MEAN;
	public String RESV_MAX;
	public String RESV_MIN;
	public String RESV_DISTRIBUTION;
	public String RESV_KURTOSIS;
	public String RESV_SKEWNESS;

	@Override
	public void readFields(ResultSet result) throws SQLException {
		// TODO Auto-generated method stub
		this.CHAR_ID = result.getString(1);

		COUNT = result.getString(2);
		DISTINCTVALUES = result.getString(3);
		STD = result.getString(4);

		MEDIAN = result.getString(5);
		MEAN = result.getString(6);
		MAX = result.getString(7);
		MIN = result.getString(8);
		DISTRIBUTION = result.getString(9);
		KURTOSIS = result.getString(10);
		SKEWNESS = result.getString(11);

		RESV_COUNT = result.getString(12);
		RESV_DISTINCTVALUES = result.getString(13);
		RESV_STD = result.getString(14);
		RESV_MEDIAN = result.getString(15);
		RESV_MEAN = result.getString(16);
		RESV_MAX = result.getString(17);
		RESV_MIN = result.getString(18);
		RESV_DISTRIBUTION = result.getString(19);
		RESV_KURTOSIS = result.getString(20);
		RESV_SKEWNESS = result.getString(21);

	}

	@Override
	public void write(PreparedStatement result) throws SQLException {
		// TODO Auto-generated method stub
		result.setString(1, CHAR_ID);
		result.setString(2, COUNT);
		result.setString(3, DISTINCTVALUES);
		result.setString(4, STD);

		result.setString(5, MEDIAN);
		result.setString(6, MEAN);
		result.setString(7, MAX);
		result.setString(8, MIN);
		result.setString(9, DISTRIBUTION);
		result.setString(10, KURTOSIS);
		result.setString(11, SKEWNESS);

		result.setString(12, RESV_COUNT);
		result.setString(13, RESV_DISTINCTVALUES);
		result.setString(14, RESV_STD);
		result.setString(15, RESV_MEDIAN);
		result.setString(16, RESV_MEAN);
		result.setString(17, RESV_MAX);
		result.setString(18, RESV_MIN);
		result.setString(19, RESV_DISTRIBUTION);
		result.setString(20, RESV_KURTOSIS);
		result.setString(21, RESV_SKEWNESS);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.CHAR_ID = Text.readString(in);

		COUNT = Text.readString(in);
		DISTINCTVALUES = Text.readString(in);
		STD = Text.readString(in);

		MEDIAN = Text.readString(in);
		MEAN = Text.readString(in);
		MAX = Text.readString(in);
		MIN = Text.readString(in);
		DISTRIBUTION = Text.readString(in);
		KURTOSIS = Text.readString(in);
		SKEWNESS = Text.readString(in);

		RESV_COUNT = Text.readString(in);
		RESV_DISTINCTVALUES = Text.readString(in);
		RESV_STD = Text.readString(in);
		RESV_MEDIAN = Text.readString(in);
		RESV_MEAN = Text.readString(in);
		RESV_MAX = Text.readString(in);
		RESV_MIN = Text.readString(in);
		RESV_DISTRIBUTION = Text.readString(in);
		RESV_KURTOSIS = Text.readString(in);
		RESV_SKEWNESS = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Text.writeString(out, this.CHAR_ID);
		Text.writeString(out,COUNT);
		Text.writeString(out,DISTINCTVALUES);
		Text.writeString(out,STD);
		Text.writeString(out,MEDIAN);
		Text.writeString(out,MEAN);
		Text.writeString(out,MAX);
		Text.writeString(out,MIN);
		Text.writeString(out, DISTRIBUTION);
		Text.writeString(out,KURTOSIS);
		Text.writeString(out,SKEWNESS);

		Text.writeString(out,RESV_COUNT);
		Text.writeString(out,RESV_DISTINCTVALUES);
		Text.writeString(out,RESV_STD);
		Text.writeString(out,RESV_MEDIAN);
		Text.writeString(out,RESV_MEAN);
		Text.writeString(out,RESV_MAX);
		Text.writeString(out,RESV_MIN);
		Text.writeString(out, RESV_DISTRIBUTION);
		Text.writeString(out,RESV_KURTOSIS);
		Text.writeString(out,RESV_SKEWNESS);
	}

}
