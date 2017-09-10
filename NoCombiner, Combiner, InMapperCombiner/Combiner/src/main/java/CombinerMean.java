import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
* If a stationId does not have tMaxAvg or tMinAvg (it should atleast have one
* of them) it is represented as 'None'
*/

public class CombinerMean {

    public static void main(String[] args) throws Exception {

        /* Sets the configuration for the job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CombinerMean");
        job.setJarByClass(CombinerMean.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(TemperatureMeanCombiner.class);
        job.setReducerClass(MeanReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TemperatureData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    /*
    * Mapper Class
    * Input is each record in the csv file
    * Output: It emits StationId as the key and (TMax, TMaxCount, TMin, TminCount)
    *         as value.
    *         Here, if the value object contains value for TMax, TMaxCount = 1, while
    *               TMin and TMinCount will be 0
    *         and if the value object contains value for TMin, TMinCount = 1, while
    *             TMax and TMaxCount will be 0
    */
    public static class TemperatureMapper
            extends Mapper<Object, Text, Text, TemperatureData> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            /*Checks if the record contains TMAX or TMIN*/
            if(line.contains("TMAX") || line.contains("TMIN")){
                String[] temp = new String[5];

                /* Extracts TMAX/TMIN for stationId */
                temp = line.split(",");
                String stationId = temp[0];
                TemperatureData tempDataObject = new TemperatureData();

                if(temp[2].equals("TMAX")){
                    tempDataObject.TMax = Float.parseFloat(temp[3]);
                    tempDataObject.TMaxCount = 1;
                }
                else{
                    if(temp[2].equals("TMIN")){
                        tempDataObject.TMin = Float.parseFloat(temp[3]);
                        tempDataObject.TMinCount = 1;
                    }
                }

                context.write(new Text(stationId), tempDataObject);
            }
        }
    }


    /*
    * Combiner Class
    * Input: ((stationId) as key, [(tMax, tMaxCt, tMin, tMinCt), ...])
    * It calculates the sum and count for tmax and tmin for each stationId
    * Output: It emits (stationId) as key and (tMaxSum, tMaxCt, tMinSum, tMinCt)
    *         as value
    */
    public static class TemperatureMeanCombiner
            extends Reducer<Text,TemperatureData,Text,TemperatureData> {

        @Override
        public void reduce(Text key, Iterable<TemperatureData> tempValues, Context context)
                throws IOException, InterruptedException {

            /* Calculates sum and count for tmax and tmin*/
            int tMaxCount = 0, tMinCount = 0;
            float tMaxSum = 0, tMinSum = 0;
            for (TemperatureData obj : tempValues) {
                if(obj.TMaxCount != 0){
                    tMaxCount++;
                    tMaxSum += obj.TMax;
                }
                if(obj.TMinCount != 0){
                    tMinCount++;
                    tMinSum += obj.TMin;
                }
            }

            TemperatureData tempDataObject = new TemperatureData();
            tempDataObject.TMax = tMaxSum;
            tempDataObject.TMaxCount = tMaxCount;
            tempDataObject.TMin = tMinSum;
            tempDataObject.TMinCount = tMinCount;

            context.write(key, tempDataObject);
        }
    }


    /*
    * Reducer Class
    * Input: ((stationId) as key, [(tMax, tMaxCt, tMin, tMinCt), ...])
    * It calculates the average TMAX and TMIN for each stationId
    * Each reduce call will receive all the values (List<TempeartureData>) for
    * same stationId
    * Output: stationId, tMinAvg, tMaxAvg
    */
    public static class MeanReducer
            extends Reducer<Text,TemperatureData,Text,NullWritable> {

        @Override
        public void reduce(Text key, Iterable<TemperatureData> tempValues, Context context)
                throws IOException, InterruptedException {

            /* Stores the output string */
            String outputValue = key + ", ";

            /* Computes average for given stationId (key) */
            int tMaxCount = 0, tMinCount = 0;
            float tMaxSum = 0, tMinSum = 0, tMaxAvg = -1, tMinAvg = -1;
            for (TemperatureData obj : tempValues) {
                if(obj.TMaxCount != 0){
                    tMaxCount += obj.TMaxCount;
                    tMaxSum += obj.TMax;
                }
                if(obj.TMinCount != 0){
                    tMinCount += obj.TMinCount;
                    tMinSum += obj.TMin;
                }
            }
            if(tMaxCount != 0)tMaxAvg = tMaxSum/tMaxCount;
            if(tMinCount != 0)tMinAvg = tMinSum/tMinCount;

            outputValue += (tMinAvg == -1? "None" : Float.toString(tMinAvg)) + ", "
                    + (tMaxAvg == -1? "None" : Float.toString(tMaxAvg));

            context.write(new Text(outputValue), NullWritable.get());
        }
    }
}
