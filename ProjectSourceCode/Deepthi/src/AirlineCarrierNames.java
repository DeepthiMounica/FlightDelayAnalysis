

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AirlineCarrierNames {

    public static class AirlineCarrierNamesMapper extends Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, DoubleWritable>{

        @Override
        protected void map(Object key, org.apache.hadoop.io.Text value, Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            if (!value.toString().contains("ARR_DELAY_NEW")) {

                String[] input = value.toString().split(",");

                if(input.length >7 && !input[7].equalsIgnoreCase("0") && !input[15].equalsIgnoreCase("0")){

                    org.apache.hadoop.io.Text flightCarrier = new org.apache.hadoop.io.Text();
                    try{

                        flightCarrier.set(input[3].trim());
                        context.write(flightCarrier, new DoubleWritable(Double.parseDouble(input[30])));


                    } catch (Exception ex) {
                       //throw ex;
                        //UniqueCarrier.set(input[2]);
                        //context.write(UniqueCarrier, new AverageBean(0, 0));
                    }
                }
            }
        }

    }


    public static class AirlineCarrierNamesReducer extends Reducer<org.apache.hadoop.io.Text, DoubleWritable, org.apache.hadoop.io.Text, DoubleWritable> {

        @Override
        protected void reduce(org.apache.hadoop.io.Text key, Iterable<DoubleWritable> values,Reducer<org.apache.hadoop.io.Text, DoubleWritable, org.apache.hadoop.io.Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            double count = 0;

            for (DoubleWritable w : values) count += w.get();

            context.write(key, new DoubleWritable(count));
        }

    }


    public static void main( String[] args ){

        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "Airline Carrier Names!");
            job.setJarByClass(AirlineCarrierNames.class);
            job.setMapperClass(AirlineCarrierNamesMapper.class);
            //job.setCombinerClass(Summarization_Reducer.class);
            job.setReducerClass(AirlineCarrierNamesReducer.class);

            job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}