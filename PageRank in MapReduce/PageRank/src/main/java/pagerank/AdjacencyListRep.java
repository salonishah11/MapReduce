package pagerank;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/*
    Class stores the Adjacency List of a node
*/
public class AdjacencyListRep implements Writable {

    ArrayList<String> AdjacencyList = new ArrayList<String>();

    public AdjacencyListRep(){
    }

    /* Writes the value of class variables to the data output */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVInt(dataOutput, AdjacencyList.size());
        for (String s : AdjacencyList) {
            WritableUtils.writeString(dataOutput, s);
        }
    }

    /* Reads the value of elements in list from data input */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = WritableUtils.readVInt(dataInput);
        AdjacencyList = new ArrayList<String>();
        for (int i = 0; i < size; i++){
            AdjacencyList.add(WritableUtils.readString(dataInput));
        }
    }
}
