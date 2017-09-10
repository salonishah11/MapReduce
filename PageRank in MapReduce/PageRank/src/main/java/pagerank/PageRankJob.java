package pagerank;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/*
    Class contains Mapper and Reducer to calculate PageRank
*/
public class PageRankJob {

    /*
        Mapper Class
        Input is each record in the (pre-proccessed) file
        Output: Emits (pagename, pagerank). And if the node is not a dangling node,
        for each page in its adjacency list, it emits (pagename, outlinksContribution).
        If it is a dangling node, it emits a dummy key and it's pagerank    *
    */
    public static class PageRankMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            GraphVertexData node = new GraphVertexData().ConvertToVertexRep(value.toString());

            double deltaIndividual = 0;
            double danglingNodeSum = Double.parseDouble(context.getConfiguration().get("Delta"));
            double totalNodes = Double.parseDouble(context.getConfiguration().get("Total_Nodes"));

            /*
                First iteration of PageRank
                Initial PageRank of node equals 1/TotalNodes
            */
            if(node.PageRank == -1){
                node.PageRank = 1/totalNodes;
            }

            /* Corrects the pagerank of node using Delta */
            if(danglingNodeSum != -1){
                double ALPHA = Double.parseDouble(context.getConfiguration().get("Alpha"));
                deltaIndividual = danglingNodeSum/totalNodes;
                node.PageRank += (1-ALPHA) * deltaIndividual;
            }

            context.write(new Text(node.PageName), new Text(node.toString()));

            /* If node is not a dangling node, it emits the page and its outlinksContribution */
            if(node.AdjacencyList.size() != 0){
                double outlinksPageRank = node.PageRank/node.AdjacencyList.size();

                for (String page : node.AdjacencyList) {
                    context.write(new Text(page), new Text(Double.toString(outlinksPageRank)));
                }
            }
            else{
                /* Dangling node handling */
                context.write(new Text("DanglingNode"), new Text(Double.toString(node.PageRank)));
            }
        }
    }


    /*
        Combiner Class
        Input: (pagename, List<text>)
        Output: If the key received is a dangling node, it sums the values, and emits it
         If the key is not a dangling node, it sums the inlink contibutions received, and emits it
     */
    public static class PageRankCombiner
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            /* Dangling node */
            if(key.toString().equals("DanglingNode")){
                double danglingNodeSum = 0;

                for (Text value : values) {
                    try {
                        double pagerank = Double.parseDouble(value.toString());
                        danglingNodeSum += pagerank;
                    } catch (NumberFormatException e) {}
                }

                context.write(new Text("DanglingNode"), new Text(Double.toString(danglingNodeSum)));
            }
            else{
                double pagerankSum = 0;
                GraphVertexData node = null;

                for (Text value : values) {
                    try {
                        double pagerank = Double.parseDouble(value.toString());
                        pagerankSum += pagerank;
                    } catch (NumberFormatException e) {
                        /* Node found */
                        node = new GraphVertexData().ConvertToVertexRep(value.toString());
                        context.write(new Text(node.PageName), new Text(node.toString()));
                    }
                }

                if(pagerankSum != 0){
                    context.write(key, new Text(Double.toString(pagerankSum)));
                }
            }
        }
    }


    /*
        Reducer Class
        Input: (pagename, List<text>)
        Output: If the key received is a dummy key (dangling node), it calculates its sum,
        and updates the global counter maintaining dangling node sum. Otherwise, it
        sums the inlinks contribution, calculates the pagerank, and emits it
    */
    public static class PageRankReducer
            extends Reducer<Text, Text, NullWritable, Text>{

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if(key.toString().equals("DanglingNode")){
                double danglingNodeSum = 0;

                for (Text value : values) {
                    try {
                        double pagerank = Double.parseDouble(value.toString());
                        danglingNodeSum += pagerank;
                    } catch (NumberFormatException e) {}
                }


                context.getCounter(PageRank.DanglingNodeCounter.DANGLING_NODE_SUM)
                        .setValue(Double.doubleToLongBits(danglingNodeSum));
            }
            else{
                double ALPHA = Double.parseDouble(context.getConfiguration().get("Alpha"));
                double TOTAL_NODES = Double.parseDouble(context.getConfiguration().get("Total_Nodes"));
                double pagerankSum = 0;
                GraphVertexData node = null;

                for (Text value : values) {
                    try {
                        double pagerank = Double.parseDouble(value.toString());
                        pagerankSum += pagerank;
                    } catch (NumberFormatException e) {
                        //Vertex found
                        node = new GraphVertexData().ConvertToVertexRep(value.toString());
                    }
                }

                if(node != null){
                    node.PageRank = (ALPHA/TOTAL_NODES) + (1-ALPHA) * pagerankSum;

                    context.write(NullWritable.get(), new Text(node.toString()));
                }
            }
        }
    }
}
