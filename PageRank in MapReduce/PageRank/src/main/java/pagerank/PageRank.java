package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    Main Class
    Sets configuration for all jobs
*/
public class PageRank {

    public static final int ITERATION_CT = 10;
    public static long TOTAL_NODES;
    public static double DELTA = -1;

    /* Global counter to count total number of pages */
    public enum PageRankCounter{
        TOTAL_NODES
    }

    /* Global counter to maintaing dangling nodes sum per iteration */
    public enum DanglingNodeCounter{
        DANGLING_NODE_SUM
    }



    public static void main(String[] args) throws Exception {

        /* PRE-PROCESSING JOB */
        /* Sets the configuration for the job */
        Configuration preProcessingConf = new Configuration();
        Job preProcessingJob = Job.getInstance(preProcessingConf, "PreProcessingJob");

        preProcessingJob.setJarByClass(PageRank.class);
        preProcessingJob.setMapperClass(PreProcessing.PreProcessingMapper.class);
        preProcessingJob.setCombinerClass(pagerank.PreProcessing.PreProcessingCombiner.class);
        preProcessingJob.setReducerClass(PreProcessing.PreProcessingReducer.class);
        preProcessingJob.setMapOutputKeyClass(Text.class);
        preProcessingJob.setMapOutputValueClass(AdjacencyListRep.class);
        preProcessingJob.setOutputKeyClass(NullWritable.class);
        preProcessingJob.setOutputValueClass(GraphVertexData.class);

        FileInputFormat.addInputPath(preProcessingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preProcessingJob, new Path(args[1] + "/" + 0));

        preProcessingJob.waitForCompletion(true);

        /* Sets TOTAL_NODES */
        Counter counter = preProcessingJob.getCounters().findCounter(PageRankCounter.TOTAL_NODES);
        TOTAL_NODES = counter.getValue();


        /* PAGERANK JOB (10 iterations) */
        int iterationNo = 1;
        while(iterationNo <= ITERATION_CT){

            /* Sets the configuration for the job */
            Configuration pagerankConf = new Configuration();
            /*
                Value for total node, alpha and delta (i-1 iteration, default is -1) is passed
                into job configuration to be accessed by Mappers and Reducers
            */
            pagerankConf.setDouble("Total_Nodes", TOTAL_NODES);
            pagerankConf.setDouble("Alpha", 0.15);
            pagerankConf.setDouble("Delta", DELTA);

            Job pagerankJob = Job.getInstance(pagerankConf, "PageRank Iteration");

            pagerankJob.setJarByClass(PageRank.class);
            pagerankJob.setMapperClass(PageRankJob.PageRankMapper.class);
            pagerankJob.setCombinerClass(PageRankJob.PageRankCombiner.class);
            pagerankJob.setReducerClass(PageRankJob.PageRankReducer.class);

            pagerankJob.setMapOutputKeyClass(Text.class);
            pagerankJob.setMapOutputValueClass(Text.class);
            pagerankJob.setOutputKeyClass(NullWritable.class);
            pagerankJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(pagerankJob, new Path(args[1] + "/" + (iterationNo-1)));
            FileOutputFormat.setOutputPath(pagerankJob, new Path(args[1] + "/" + iterationNo));

            pagerankJob.waitForCompletion(true);

            /* Sets Delta as sum of dangling nodes contribution for ith iteration*/
            DELTA = Double.longBitsToDouble(pagerankJob.getCounters().
                    findCounter(DanglingNodeCounter.DANGLING_NODE_SUM).getValue());

            iterationNo++;
        }


        /* Final Map job to correct the PageRank of each node */
        /* Sets the configuration for the job */
        Configuration finalPagerankConf = new Configuration();
        finalPagerankConf.setDouble("Total_Nodes", TOTAL_NODES);
        finalPagerankConf.setDouble("Alpha", 0.15);
        finalPagerankConf.setDouble("Delta", DELTA);

        Job finalPagerankJob = Job.getInstance(finalPagerankConf, "PageRank Final Iteration");

        finalPagerankJob.setJarByClass(PageRank.class);
        finalPagerankJob.setMapperClass(FinalPageRankCalc.CorrectPageRankMapper.class);
        /* Number of Reduce tasks is set to 0 */
        finalPagerankJob.setNumReduceTasks(0);

        finalPagerankJob.setMapOutputKeyClass(NullWritable.class);
        finalPagerankJob.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(finalPagerankJob, new Path(args[1] + "/" + (iterationNo-1)));
        FileOutputFormat.setOutputPath(finalPagerankJob, new Path(args[1] + "/" + iterationNo));

        finalPagerankJob.waitForCompletion(true);


        /* TOP-K JOB (top 100) */
        /* Sets the configuration for the job */
        Configuration topKConf = new Configuration();
        Job topKJob = Job.getInstance(topKConf, "TopKJob");

        topKJob.setJarByClass(PageRank.class);
        topKJob.setMapperClass(TopKJob.TopKMapper.class);
        topKJob.setReducerClass(TopKJob.TopKReducer.class);
        /* Single Reduce Tasks */
        topKJob.setNumReduceTasks(1);

        topKJob.setMapOutputKeyClass(Text.class);
        topKJob.setMapOutputValueClass(Text.class);
        topKJob.setOutputKeyClass(NullWritable.class);
        topKJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(topKJob, new Path(args[1] + "/" + iterationNo));
        FileOutputFormat.setOutputPath(topKJob, new Path(args[1] + "/Top100_PageRank"));

        topKJob.waitForCompletion(true);

    }
}
