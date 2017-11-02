package com.refactorlabs.cs378.assign7;

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class A7 extends Configured implements Tool {

    /**
     * Mapper for Session Avro Objects
     * */
    public static class SessionsMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        /**
         * Local variable "vin" will contain the word identified in the input.
         */
        private AvroMultipleOutputs multipleOutputs;
        private static Random rands = new Random();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            Session session = value.datum();

            // Filter Out All Sessions with more than 100 events
            if(session.getEvents().size() <= 100) {
                SessionType st = SessionType.OTHER; //Default to Other
                int best = 5;
                for(Event event : session.getEvents()) {
                    EventType type = event.getEventType();
//                    EventSubtype subtype = event.getEventSubtype();
                    int current = val(type);
                    if (current < best) {
                        best = current;
                        st = convert(type);
                    }
                    if (st == SessionType.SUBMITTER) // Already of Highest Priority so stop loop
                        break;
                }
                // Filter Out 90% of CLICKER sessions
                if (st == SessionType.CLICKER) {
                    if (rands.nextInt(100) >= 10) {
                        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Discarded Clicker Sessions").increment(1L);
                        return;
                    }
                }
                // Filter Out 98% of SHOWER sessions
                else if (st == SessionType.SHOWER) {
                    if (rands.nextInt(100) >= 2) {
                        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Discarded Shower Sessions").increment(1L);
                        return;
                    }
                }

                multipleOutputs.write(st.getText(), key, value);
            }
            // Increment counter for large sessions
            else
                context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Discarded Large Sessions").increment(1L);
        }

        @Override
        public void setup(Context context) {
            multipleOutputs = new AvroMultipleOutputs(context);
        }

        @Override
        public void cleanup(Context context) throws InterruptedException, IOException {
            multipleOutputs.close();
        }

        //Converts EventType to int used for comparing
        private static int val(EventType et) {
            if (et == EventType.SUBMIT || et == EventType.CHANGE || et == EventType.EDIT)
                return 1;

            else if (et == EventType.CLICK)
                return 2;

            else if (et == EventType.SHOW || et == EventType.DISPLAY)
                return 3;

            else if (et == EventType.VISIT)
                return 4;

            else //OTHER
                return 5;
        }

        //Converts EventType to SessionType
        private static SessionType convert(EventType et) {
            if (et == EventType.SUBMIT || et == EventType.CHANGE || et == EventType.EDIT)
                return SessionType.SUBMITTER;

            else if (et == EventType.CLICK)
                return SessionType.CLICKER;

            else if (et == EventType.SHOW || et == EventType.DISPLAY)
                return SessionType.SHOWER;

            else if (et == EventType.VISIT)
                return SessionType.VISITOR;

            else //OTHER
                return SessionType.OTHER;
        }

    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: A7 <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "A7");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(A7.class);

        // Specify the Map
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
//        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setNumReduceTasks(0);

        // Avro MultipleOutputs
        AvroMultipleOutputs.addNamedOutput(job, SessionType.SUBMITTER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.CLICKER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.SHOWER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.VISITOR.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.OTHER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.setCountersEnabled(job, true);

        // Grab the input file and output directory from the command line.
        MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

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
        int res = ToolRunner.run(new A7(), args);
        System.exit(res);
    }
}
