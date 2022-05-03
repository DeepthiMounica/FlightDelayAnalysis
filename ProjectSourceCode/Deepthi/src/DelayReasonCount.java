

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//Avg monthly arrival delay trend.
public class DelayReasonCount {

    public static class DelayReasonCountMapper extends Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, NullWritable>{

        private MultipleOutputs<org.apache.hadoop.io.Text, IntWritable> mos=null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void map(Object key, org.apache.hadoop.io.Text value, Context context)
                throws IOException, InterruptedException {


            if (!value.toString().contains("ARR_DELAY_NEW")) {

                String[] input = value.toString().split(",");

                if(input.length >7){

                    org.apache.hadoop.io.Text month = new org.apache.hadoop.io.Text();
                    try{

                        double carrier_delay = Double.parseDouble(input[38].trim());
                        double weather_delay = Double.parseDouble(input[39].trim());
                        double nas_delay = Double.parseDouble(input[40].trim());
                        double security_delay =Double.parseDouble(input[41].trim());


                        if(carrier_delay > 0)
                            mos.write("bins", value, NullWritable.get(),"Carrier-cancellation");
                        if(weather_delay > 0)
                            mos.write("bins", value, NullWritable.get(),"Weather-cancellation");
                        if(nas_delay > 0)
                            mos.write("bins", value, NullWritable.get(),"NAS-cancellation");
                        if(security_delay > 0)
                            mos.write("bins", value, NullWritable.get(),"Security-cancellation");


                    } catch (Exception ex) {
                        //throw ex;
                        //UniqueCarrier.set(input[2]);
                        //context.write(UniqueCarrier, new AverageBean(0, 0));
                    }
                }
            }
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

    }



    public static void main( String[] args ){

        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "Monthly Delays trends!");
            job.setJarByClass(DelayReasonCount.class);
            job.setMapperClass(DelayReasonCountMapper.class);
            //job.setReducerClass(DelayReasonCountReducer.class);

            job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(NullWritable.class);


            MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, org.apache.hadoop.io.Text.class, IntWritable.class);
            MultipleOutputs.setCountersEnabled(job, true);

            job.setNumReduceTasks(0);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
