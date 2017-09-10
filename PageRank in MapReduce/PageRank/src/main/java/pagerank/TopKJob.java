package pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/*
    Class comtains Mapper and Reducer classes to run the Top-K Job
*/
public class TopKJob {

    /* Value of K */
    private static int NUM_OF_TOP_RECORDS = 100;

    public static class PageRankComparator implements Comparator<GraphVertexData> {

        @Override
        public int compare(GraphVertexData v1, GraphVertexData v2) {
            if(v1.PageRank > v2.PageRank)return 1;

            return -1;
        }
    }

    /*
        Mapper Class
        Emits the top local 100 nodes having highest pagerank
    */
    public static class TopKMapper
            extends Mapper<Object, Text, Text, Text> {

        private PriorityQueue<GraphVertexData> TopKRecords;

        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            Comparator<GraphVertexData> comparator = new PageRankComparator();
            TopKRecords = new PriorityQueue<GraphVertexData>(comparator);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            GraphVertexData node = new GraphVertexData().ConvertToVertexRep(value.toString());
            node.AdjacencyList = new ArrayList<>();

            TopKRecords.add(node);

            /*
                If size goes beyond 100, we need to remove the page with lowest pagerank
                in the queue
            */
            if(TopKRecords.size() > NUM_OF_TOP_RECORDS){
                TopKRecords.poll();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{

            /* Emits the local top 100 */
            while(!TopKRecords.isEmpty()){
                GraphVertexData node = TopKRecords.poll();
                if(node != null) {
                    context.write(new Text("Top" + NUM_OF_TOP_RECORDS), new Text(node.toString()));
                }
            }
        }
    }


    /*
        Reducer class
        It receives top 100 from all mappers, and calculates top 100 in same manner as above.
        It emits the final top 100 pages having pagerank from highest to lowest
    */
    public static class TopKReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        private PriorityQueue<GraphVertexData> TopKRecords;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Comparator<GraphVertexData> comparator = new PageRankComparator();
            TopKRecords = new PriorityQueue<GraphVertexData>(comparator);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                GraphVertexData node = new GraphVertexData().ConvertToVertexRep(value.toString());

                TopKRecords.add(node);

                /*
                If size goes beyond 100, we need to remove the page with lowest pagerank
                in the queue
                */
                if (TopKRecords.size() > NUM_OF_TOP_RECORDS) {
                    TopKRecords.poll();
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{

            GraphVertexData[] top100Array = new GraphVertexData[NUM_OF_TOP_RECORDS];
            int i = NUM_OF_TOP_RECORDS - 1;

            /*
                Since queue stores pages in ascending order, we need to reverse it
            */
            while(!TopKRecords.isEmpty()){
                GraphVertexData node = TopKRecords.poll();
                if(node != null) {
                    top100Array[i] = node;
                    i--;
                }
            }

            /* Emits the local top 100 */
            for (GraphVertexData node : top100Array) {
                context.write(NullWritable.get(), new Text(node.PageName + "    " + node.PageRank));
            }
        }
    }
}
