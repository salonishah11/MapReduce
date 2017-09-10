import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
Class represents the key structure
(StationId, Year) acts as the key for this program
*/
public class StationYearData implements Writable, WritableComparable<StationYearData> {

    String StationId;
    int Year;

    public StationYearData(){
    }

    /* Writes the value of class variables to the data output */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, StationId);
        WritableUtils.writeVInt(dataOutput, Year);
    }

    /* Reads the value of stationId and year from data input */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        StationId = WritableUtils.readString(dataInput);
        Year = WritableUtils.readVInt(dataInput);
    }

    /*
    Compares only the stationId for any object of this that uses compareTo
    This method is called inside both NaturalComparator and GroupingComparator
    */
    @Override
    public int compareTo(StationYearData st) {
        return StationId.compareTo(st.StationId);
    }
}
