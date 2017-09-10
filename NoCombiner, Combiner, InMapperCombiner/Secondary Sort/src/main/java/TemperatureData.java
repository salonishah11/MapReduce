import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
Class represents the value structure
(TMax, TMaxCt, TMin, TMinCt) acts as the value object emitted
*/
public class TemperatureData implements Writable {
    float TMax;
    int TMaxCount;
    float TMin;
    int TMinCount;

    public TemperatureData(){
    }

    /* Writes the value of class variables to the data output */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(TMax);
        dataOutput.writeInt(TMaxCount);
        dataOutput.writeFloat(TMin);
        dataOutput.writeInt(TMinCount);
    }

    /* Reads the value of stationId and year from data input */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        TMax = dataInput.readFloat();
        TMaxCount = dataInput.readInt();
        TMin = dataInput.readFloat();
        TMinCount = dataInput.readInt();
    }
}
