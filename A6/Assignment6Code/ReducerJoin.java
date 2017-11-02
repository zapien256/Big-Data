package com.refactorlabs.cs378.assign6;

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
//import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class ReducerJoin extends Configured implements Tool {

    /**
     * Mapper for Session Avro Objects
     * */
    public static class SessionsMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

        /**
         * Local variable "vin" will contain the word identified in the input.
         */
        private Text vin = new Text();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            Session session = value.datum();
            Map<String, Map<CharSequence, Long>> vins = new HashMap<>(); // <K:vin, V:subtypes of click>
            Map<String, Integer> editCounts = new HashMap<>(); // <K:vin, V: edit contact form>

            for(Event event : session.getEvents()) {
                String v = event.getVin().toString();
                String type = event.getEventType().toString();
                String subtype = event.getEventSubtype().toString();
                if(!vins.containsKey(v)) {
                    //New VIN
                    vins.put(v, new HashMap<>());
                    editCounts.put(v, 0);
                }
                if(type.toLowerCase().equals("edit") && subtype.toLowerCase().equals("contact_form")) {
                    //Edit Contact Form has occurred at least once
                    editCounts.put(v, 1);
                }
                else if(type.toLowerCase().equals("click")) {
                    //Count Unique User Clicks
                    vins.get(v).put(subtype,1L);
                }
            }

            //Build and output a VinImpressionCounts Avro object for each vin
            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            for (String K : vins.keySet()) {
                vin.set(K);
                Map<CharSequence, Long> clicks = vins.get(K);
                int edits = editCounts.get(K);
                builder.setUniqueUsers(1);
                builder.setClicks(clicks);
                builder.setEditContactForm(edits);
                builder.setMarketplaceSrps(0);
                builder.setMarketplaceVdps(0);
                context.write(vin, new AvroValue<VinImpressionCounts>(builder.build()));
            }
        }
    }

    /**
     * Mapper for CSV Files containing VinImpressionCount Data
     * */
    public static class ImpressionsMapper extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

        /**
         * Local variable "vin" will contain the userID identified in the input.
         */
        private Text vin = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split(",");

            vin.set(values[0]);
            assert(values[0] != null);
            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            if (values[1].equals("SRP")) {
                builder.setMarketplaceSrps(Integer.valueOf(values[2]));
                builder.setMarketplaceVdps(0);
            }
            else {
                builder.setMarketplaceVdps(Integer.valueOf(values[2]));
                builder.setMarketplaceSrps(0);
            }


            builder.setUniqueUsers(0);
            builder.setEditContactForm(0);
            builder.setClicks(new HashMap<CharSequence, Long>());

            context.write(vin, new AvroValue<VinImpressionCounts>(builder.build()));
        }
    }

    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
	public static class Reduce
			extends Reducer<Text, AvroValue<VinImpressionCounts>,
			                AvroKey<CharSequence>, AvroValue<VinImpressionCounts>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {

            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            long sum_uniqueUsers = 0;
            long edits = 0;
            long marketSRPs = 0;
            long marketVDPs = 0;
            Map<CharSequence, Long> clicks = new HashMap<>();

            // Sum all values for Vin Impression Stats
            for(AvroValue<VinImpressionCounts> avroValue : values) {
                VinImpressionCounts vic = avroValue.datum();
                sum_uniqueUsers += vic.getUniqueUsers();
                edits += vic.getEditContactForm();
                marketSRPs += vic.getMarketplaceSrps();
                marketVDPs += vic.getMarketplaceVdps();
                Map<CharSequence, Long> vicClicks = vic.getClicks();
                for (CharSequence K : vicClicks.keySet()) { //Sum up Clicks
                    if (!clicks.containsKey(K)) //New Subtype for 'CLICK' in clicks
                        clicks.put(K, vicClicks.get(K));
                    else //Add to existing value for key 'K' in clicks
                        clicks.put(K, clicks.get(K) + vicClicks.get(K));
                }
            }

            // Ignore any VIN found in the Right Input but not Left Input
            if(sum_uniqueUsers > 0) {
                //If VIN is on the LEFT side, the number of unique users will be > 0
                builder.setMarketplaceVdps(marketVDPs);
                builder.setMarketplaceSrps(marketSRPs);
                builder.setEditContactForm(edits);
                builder.setClicks(clicks);
                builder.setUniqueUsers(sum_uniqueUsers);
                context.write(new AvroKey<CharSequence>(key.toString()),
                        new AvroValue<VinImpressionCounts>(builder.build()));
            }
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: SessionsMR <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ReducerJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(ReducerJoin.class);

        // Specify the Map
//        job.setInputFormatClass(AvroKeyValueInputFormat.class);
//        job.setMapperClass(SessionsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(Reduce.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Grab the input file and output directory from the command line.
//        FileInputFormat.addInputPaths(job, appArgs[0]);
        MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionsMapper.class);
        MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, ImpressionsMapper.class);
        MultipleInputs.addInputPath(job, new Path(appArgs[2]), TextInputFormat.class, ImpressionsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[3]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new ReducerJoin(), args);
        System.exit(res);
    }
}
