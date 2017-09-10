package pagerank;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    This Mapper is used to correct the pagerank of pages obtained after 10th iteration.
*/
public class FinalPageRankCalc {

    public static class CorrectPageRankMapper
            extends Mapper<Object, Text, NullWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            /* Converts value to node representation */
            GraphVertexData node = new GraphVertexData().ConvertToVertexRep(value.toString());

            double deltaIndividual = 0;
            double danglingNodeSum = Double.parseDouble(context.getConfiguration().get("Delta"));
            double totalNodes = Double.parseDouble(context.getConfiguration().get("Total_Nodes"));

            /* Correct of PageRank for each node */
            if(danglingNodeSum != -1){
                double ALPHA = Double.parseDouble(context.getConfiguration().get("Alpha"));
                deltaIndividual = danglingNodeSum/totalNodes;
                node.PageRank += (1-ALPHA) * deltaIndividual;
            }

            context.write(NullWritable.get(), new Text(node.toString()));
        }
    }
}
