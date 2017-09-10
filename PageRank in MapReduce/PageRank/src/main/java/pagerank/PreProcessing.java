package pagerank;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/*
    Class contains Mapper and Reducer to do the Pre-Processing Job
*/
public class PreProcessing {

    /*
        Mapper Class
        Input: Line from bz2 file
        It parses the line, creates its adjacency list representation and emits it.
        For each node which does not have an empty adjacency list, it iterartes over the list
        and emits the pagename and empty adjacency list
    */
    public static class PreProcessingMapper
            extends Mapper<Object, Text, Text, AdjacencyListRep> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            /* Parsing */
            Bz2WikiParser parserObj = new Bz2WikiParser();
            ParsedHTMLData parsedData = parserObj.ParseLine(value.toString());

            if(parsedData != null){
                AdjacencyListRep adjList = new AdjacencyListRep();
                adjList.AdjacencyList = parsedData.LinkedPageNames;

                context.write(new Text (parsedData.PageName), adjList);

                for (String page : adjList.AdjacencyList) {
                    context.write(new Text(page), new AdjacencyListRep());
                }
            }
        }
    }


    /*
        Combiner Class
        Input: (pagename, adjacencyList)
        Output: It combines the adjacency list obtained, and emits it
    */
    public static class PreProcessingCombiner
            extends Reducer<Text, AdjacencyListRep, Text, AdjacencyListRep> {

        @Override
        public void reduce(Text key, Iterable<AdjacencyListRep> adjListValues, Context context)
                throws IOException, InterruptedException {

            AdjacencyListRep adjListObj = new AdjacencyListRep();

            for (AdjacencyListRep list : adjListValues) {
                if(list.AdjacencyList.size() == 0)
                    continue;
                else{
                    adjListObj.AdjacencyList.addAll(list.AdjacencyList);
                    break;
                }
            }

            context.write(key, adjListObj);
        }
    }


    /*
        Reducer Class
        Input: (pagename, adjacencyList)
        Output: It combines the adjacency list obtained, and emits it
    */
    public static class PreProcessingReducer
            extends Reducer<Text, AdjacencyListRep, NullWritable, GraphVertexData> {

        @Override
        public void reduce(Text key, Iterable<AdjacencyListRep> adjListValues, Context context)
                throws IOException, InterruptedException {

            GraphVertexData vertex = new GraphVertexData();
            vertex.PageName = key.toString();
            vertex.PageRank = -1;

            for (AdjacencyListRep list : adjListValues) {
                if(list.AdjacencyList.size() == 0)
                    continue;
                else{
                    vertex.AdjacencyList.addAll(list.AdjacencyList);
                    break;
                }
            }

            /* Updates the Total nodes counter */
            context.getCounter(PageRank.PageRankCounter.TOTAL_NODES).increment(1);

            context.write(NullWritable.get(), vertex);
        }
    }
}
