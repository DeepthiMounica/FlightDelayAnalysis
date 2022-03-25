

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgDelayByCarrierNew {

    public static class AverageDelayMapper extends Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, AverageBean>{

        @Override
        protected void map(Object key, org.apache.hadoop.io.Text value, Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, AverageBean>.Context context)
                throws IOException, InterruptedException {

            if (!value.toString().contains("ARR_DELAY_NEW")) {

                String[] input = value.toString().split(",");

                if(input.length > 38 && (input[29].equalsIgnoreCase("1.0")  || input[31].equalsIgnoreCase("1") )){

                    org.apache.hadoop.io.Text uniqueCarrier = new org.apache.hadoop.io.Text();

                    try{

                        double totalFlightCount = Double.parseDouble(input[36]);

                        double delayInMin = Double.parseDouble(input[28]);

                        AverageBean outTuple= new AverageBean(totalFlightCount, delayInMin);

                        uniqueCarrier.set(input[3].trim());

                        context.write(uniqueCarrier, outTuple);


                    } catch (NumberFormatException ex) {
                        System.out.println("Skip invalid records..");
                    }
                }
            }
        }

    }

    public static class AverageDelayReducer extends Reducer<org.apache.hadoop.io.Text, AverageBean, org.apache.hadoop.io.Text, Double> {

        @Override
        protected void reduce(org.apache.hadoop.io.Text key, Iterable<AverageBean> values,Reducer<org.apache.hadoop.io.Text, AverageBean, org.apache.hadoop.io.Text, Double>.Context context)
                throws IOException, InterruptedException {
            double flightCount = 0;
            double delayInMinutes = 0;

            for (AverageBean val : values) {
                flightCount += val.getTotalFlightCount();
                delayInMinutes +=val.getDelayInMinutes();
            }
            context.write(key, delayInMinutes/flightCount);
        }

    }


    public static void main( String[] args ){

        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "Average Arrival Delay New");
            job.setJarByClass(AvgDelayByCarrierNew.class);
            job.setMapperClass(AverageDelayMapper.class);
            job.setReducerClass(AverageDelayReducer.class);

            job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setMapOutputValueClass(AverageBean.class);

            job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(Double.class);

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}