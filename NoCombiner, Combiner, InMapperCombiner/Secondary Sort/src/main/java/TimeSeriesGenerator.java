import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
* If a stationId does not have tMaxAvg or tMinAvg (it should atleast have one
* of them) it is represented as 'None'
*/

public class TimeSeriesGenerator {

    public static void main(String[] args) throws Exception {

        /* Sets the configuration for the job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TimeSeriesGenerator");

        job.setJarByClass(TimeSeriesGenerator.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(TemperatureMeanCombiner.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setSortComparatorClass(NaturalComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setReducerClass(MeanReducer.class);

        job.setMapOutputKeyClass(StationYearData.class);
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
    * Output: It emits (StationId, Year) as the key and (TMax, TMaxCount, TMin, TminCount)
    *         as value.
    *         Here, if the value object contains value for TMax, TMaxCount = 1, while
    *               TMin and TMinCount will be 0
    *         and if the value object contains value for TMin, TMinCount = 1, while
    *             TMax and TMaxCount will be 0
    */
    public static class TemperatureMapper
            extends Mapper<Object, Text, StationYearData, TemperatureData> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            /*Checks if the record contains TMAX or TMIN*/
            if(line.contains("TMAX") || line.contains("TMIN")) {
                String[] temp = new String[5];
                temp = line.split(",");

                /* Extracts TMAX/TMIN for stationId */
                String stationId = temp[0];
                int year = Integer.parseInt(temp[1].substring(0, 4));
                StationYearData stationYearDataObject = new StationYearData();
                stationYearDataObject.StationId = stationId;
                stationYearDataObject.Year = year;

                TemperatureData tempDataObject = new TemperatureData();

                if (temp[2].equals("TMAX")) {
                    tempDataObject.TMax = Float.parseFloat(temp[3]);
                    tempDataObject.TMaxCount = 1;
                }
                if (temp[2].equals("TMIN")) {
                    tempDataObject.TMin = Float.parseFloat(temp[3]);
                    tempDataObject.TMinCount = 1;
                }

                context.write(stationYearDataObject, tempDataObject);
            }
        }
    }


    /*
    * Combiner Class
    * Input: ((stationId, year) as key, [(tMax, tMaxCt, tMin, tMinCt), ...])
    * It calculates the sum and count for tmax and tmin for each (station, year)
    * Output: It emits (stationId, year) as key and (tMaxSum, tMaxCt, tMinSum, tMinCt)
    *         as value
    */
    public static class TemperatureMeanCombiner
            extends Reducer<StationYearData,TemperatureData,StationYearData,TemperatureData> {

        @Override
        public void reduce(StationYearData key, Iterable<TemperatureData> tempValues, Context context)
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
    * Partitioner Class
    * It partitions the output records emitted by Mapper solely based on
    * the stationId. The number of partitions depends on the number of
    * reduce tasks
    */
    public static class HashPartitioner extends
            Partitioner<StationYearData, TemperatureData> {

        @Override
        public int getPartition(StationYearData key, TemperatureData value,
                                int numReduceTasks) {

            /* Partitions based on stationId */
            return ((key.StationId.hashCode() & Integer.MAX_VALUE) % numReduceTasks);
        }
    }


    /*
    * NaturalComparator class
    * It is used by Reducer to sort the input list it receives. NaturalComparator
    * sorts by both stationId and year. If the stationId is equal it sorts by
    * year in ascending order
    */
    public static class NaturalComparator extends WritableComparator{

        protected NaturalComparator() {
            super(StationYearData.class, true);
        }

        @Override
        public int compare(WritableComparable st1, WritableComparable st2){

            StationYearData key1 = (StationYearData) st1;
            StationYearData key2 = (StationYearData) st2;

            /*
            * Compare stationId
            * It uses overriden method compareTo of StationYearData class
            */
            int stationCmp = key1.compareTo(key2);

            /* If same stationId compare year */
            if(stationCmp == 0){
                /*Sort Year in ascending manner*/
                if(key1.Year == key2.Year)return 0;
                if(key1.Year > key2.Year)return 1;
                return -1;
            }

            return stationCmp;
        }
    }


    /*
    * GroupingComparator Class
    * It is used by Reducer to decide the partitions inside the input list i.e how
    * long to iterate until new key occurs.
    * It compares based only stationId. Hence all the keys with same stationId will
    * be grouped into a single reduce call
    */
    public static class GroupingComparator extends WritableComparator{

        protected GroupingComparator() {
            super(StationYearData.class, true);
        }

        @Override
        public int compare(WritableComparable st1, WritableComparable st2){

            StationYearData key1 = (StationYearData) st1;
            StationYearData key2 = (StationYearData) st2;

            /*
            * Compare only StationId
            * It uses overriden method compareTo of StationYearData class
            */
            return key1.compareTo(key2);
        }
    }


    /*
    * Reducer Class
    * Input: ((stationId, year) as key, [(tMax, tMaxCt, tMin, tMinCt), ...])
    * It calculates the average TMAX and TMIN for each stationId and year
    * Each reduce call will receive all the values (List<TempeartureData>) for
    * same stationId (grouping comparator)
    * Output: stationId, [(year, tMinAvg, tMaxAvg), ....]
    */
    public static class MeanReducer extends Reducer<StationYearData, TemperatureData, Text, NullWritable>{

        @Override
        public void reduce(StationYearData key, Iterable<TemperatureData> tempValues, Context context)
                throws IOException, InterruptedException {

            /* Stores the output string */
            StringBuilder outputValue = new StringBuilder(key.StationId);
            outputValue.append(", [");
            /* Maintains the current year (to compare if new key is has occured) */
            int currentYear = key.Year;
            float tMaxSum = 0, tMinSum = 0, tMaxAvg = -1, tMinAvg = -1;
            int tMaxCount = 0, tMinCount = 0;

            for (TemperatureData tempDataObj : tempValues) {

                /*
                * Checks if key.year is same key i.e we are still calculating
                * sum and count for current (stationId, year) pair
                */
                if(key.Year != currentYear){

                    /*
                    * If key.year different, calculate average and add it to
                    * output string
                    */
                    if(tMaxCount != 0)tMaxAvg = tMaxSum/tMaxCount;
                    if(tMinCount != 0)tMinAvg = tMinSum/tMinCount;

                    outputValue.append("(" + Integer.toString(currentYear) + ", "
                            + (tMinAvg == -1? "None" : Float.toString(tMinAvg)) + ", "
                            + (tMaxAvg == -1? "None" : Float.toString(tMaxAvg)) + "), ");

                    /* Reinitialization */
                    tMaxSum = 0; tMinSum = 0; tMaxAvg = -1; tMinAvg = -1;
                    tMaxCount = 0; tMinCount = 0;

                    currentYear = key.Year;
                }


                if(tempDataObj.TMaxCount != 0){
                    tMaxCount += tempDataObj.TMaxCount;
                    tMaxSum += tempDataObj.TMax;
                }
                if(tempDataObj.TMinCount != 0){
                    tMinCount += tempDataObj.TMinCount;
                    tMinSum += tempDataObj.TMin;
                }
            }

            if(tMaxCount != 0)tMaxAvg = tMaxSum/tMaxCount;
            if(tMinCount != 0)tMinAvg = tMinSum/tMinCount;

            outputValue.append("(" + Integer.toString(currentYear) + ", "
                    + (tMinAvg == -1? "None" : Float.toString(tMinAvg)) + ", "
                    + (tMaxAvg == -1? "None" : Float.toString(tMaxAvg)) + ")");
            outputValue.append("]");

            context.write(new Text(outputValue.toString()), NullWritable.get());
        }
    }
}
