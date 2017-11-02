package com.refactorlabs.cs378.assign8;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class A8 extends Configured implements Tool {

    /**
     * Mapper for Binning Phase
     * */
    public static class SessionsMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<SessionType>, AvroValue<Session>> {

        private AvroMultipleOutputs multipleOutputs;

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
                    int current = val(type);
                    if (current < best) {
                        best = current;
                        st = convert(type);
                    }
                    if (st == SessionType.SUBMITTER) // Already of Highest Priority so stop loop
                        break;
                }
                multipleOutputs.write(st.getText(), st, value);
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
     * Mapper that determines event subtype statistics for sessions
     * */
    public static class SubmitterMapper extends Mapper<AvroKey<SessionType>, AvroValue<Session>,
            AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        private EventSubtype[] subtypes = {EventSubtype.ALTERNATIVE, EventSubtype.BADGES, EventSubtype.BADGE_DETAIL,
                EventSubtype.CONTACT_BANNER, EventSubtype.CONTACT_BUTTON, EventSubtype.CONTACT_FORM,
                EventSubtype.DEALER_PHONE, EventSubtype.FEATURES, EventSubtype.GET_DIRECTIONS,
                EventSubtype.MARKET_REPORT, EventSubtype.PHOTO_MODAL, EventSubtype.VEHICLE_HISTORY};
        private Map<EventSubtype, EventSubtypeStatisticsKey> keysMap = new HashMap<>();
        private Map<EventSubtype, EventSubtypeStatisticsData> dataMap = new HashMap<>();

        @Override
        public void map(AvroKey<SessionType> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            Session session = value.datum();
            initialize(key.datum());

            // Get stats for each event in the session
            for (Event e : session.getEvents()) {
                EventSubtype est = e.getEventSubtype();
                EventSubtypeStatisticsData data = dataMap.get(est);
                data.setTotalCount(data.getTotalCount() + 1);
            }

            // Write out each key, value pair
            for (EventSubtype est : subtypes) {
                EventSubtypeStatisticsData data = dataMap.get(est);
                calculate(data);
                context.write(new AvroKey<EventSubtypeStatisticsKey>(keysMap.get(est)),
                              new AvroValue<EventSubtypeStatisticsData>(data));
            }

        }

        // Initializes keysMap, dataMap
        private void initialize(SessionType key) {
            EventSubtypeStatisticsKey.Builder keyBuilder = EventSubtypeStatisticsKey.newBuilder();
            keyBuilder.setSessionType(key.getText());
            EventSubtypeStatisticsData.Builder dataBuilder = EventSubtypeStatisticsData.newBuilder();

            for(EventSubtype est : subtypes) {
                keyBuilder.setEventSubtype(est.toString());
                keysMap.put(est, keyBuilder.build());
                dataMap.put(est, dataBuilder.build());
            }
        }

        //Calculates stats (e.g. mean and variance) for the EventSubtypeStatisticsData object passed in
        private void calculate(EventSubtypeStatisticsData data) {
            double mean = data.getTotalCount()/(double)data.getSessionCount();
            double countSquared = data.getTotalCount()*data.getTotalCount();
            double variance = (countSquared / (double)data.getSessionCount()) - (mean*mean);
            data.setMean(mean);
            data.setVariance(variance);
        }
    }

    /**
     * The Reduce class for aggregating EventSubtypeStatisticsData
     */
    public static class ReduceClass extends Reducer<AvroKey<EventSubtypeStatisticsKey>,
            AvroValue<EventSubtypeStatisticsData>, AvroKey<EventSubtypeStatisticsKey>,
            AvroValue<EventSubtypeStatisticsData>> {

        @Override
        public void reduce(AvroKey<EventSubtypeStatisticsKey> key, Iterable<AvroValue<EventSubtypeStatisticsData>> values,
                           Context context) throws IOException, InterruptedException {

            long totalCount = 0;
            long sessionCount = 0;

            for (AvroValue<EventSubtypeStatisticsData> avroValue : values) {
                EventSubtypeStatisticsData value = avroValue.datum();
                totalCount += value.getTotalCount();
                sessionCount += value.getSessionCount();
            }

            EventSubtypeStatisticsData.Builder data = EventSubtypeStatisticsData.newBuilder();
            data.setTotalCount(totalCount);
            data.setSessionCount(sessionCount);
            calculate(data);

            EventSubtypeStatisticsKey.Builder outKey = EventSubtypeStatisticsKey.newBuilder();
            outKey.setSessionType("ANY");
            outKey.setEventSubtype("");

            // Emit the document count, mean, variance for the word.
            context.write(new AvroKey<EventSubtypeStatisticsKey>(outKey.build()), new AvroValue<EventSubtypeStatisticsData>(data.build()));
        }

        //Calculates stats (e.g. mean and variance) for the EventSubtypeStatisticsData object passed in
        private void calculate(EventSubtypeStatisticsData.Builder data) {
            double mean = data.getTotalCount()/(double)data.getSessionCount();
            double countSquared = data.getTotalCount()*data.getTotalCount();
            double variance = (countSquared / (double)data.getSessionCount()) - (mean*mean);
            data.setMean(mean);
            data.setVariance(variance);
        }
    }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: A8 <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job binningJob = Job.getInstance(conf, "A8");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        binningJob.setJarByClass(A8.class);

        // Specify the Map
        binningJob.setMapOutputKeyClass(Text.class);
        AvroJob.setInputKeySchema(binningJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(binningJob, Session.getClassSchema());
        AvroJob.setOutputKeySchema(binningJob, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(binningJob, Session.getClassSchema());

        binningJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        binningJob.setNumReduceTasks(0);

        // Avro MultipleOutputs
        AvroMultipleOutputs.addNamedOutput(binningJob, SessionType.SUBMITTER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(binningJob, SessionType.CLICKER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(binningJob, SessionType.SHOWER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(binningJob, SessionType.VISITOR.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(binningJob, SessionType.OTHER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.setCountersEnabled(binningJob, true);

        // Grab the input file and output directory from the command line.
        MultipleInputs.addInputPath(binningJob, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionsMapper.class);
        FileOutputFormat.setOutputPath(binningJob, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        binningJob.waitForCompletion(true);

        /* -----------------------------------------------------------------------------*/

        Job aggregateJob = Job.getInstance(conf, "A8");
        aggregateJob.setJarByClass(A8.class);

        //Specify Map
        aggregateJob.setMapOutputKeyClass(Text.class);
        AvroJob.setInputKeySchema(aggregateJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(aggregateJob, Session.getClassSchema());
        AvroJob.setOutputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());

        //Specify Reducer
        aggregateJob.setNumReduceTasks(1);


        //Output
        aggregateJob.setOutputFormatClass(TextOutputFormat.class);

        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new A8(), args);
        System.exit(res);
    }
}
