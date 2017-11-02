package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.Sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class SessionsMR extends Configured implements Tool {

    public static class SessionsMapper extends Mapper<LongWritable, Text, Text, AvroValue<Event>> {

        /**
         * Local variable "word" will contain the word identified in the input.
         */
        private Text user = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\t");

            String[] eventFields = values[1].split(" ", 2);
            String[] features = values[15].split(":");

            List<CharSequence> list = new ArrayList<CharSequence>();
            list.addAll(Arrays.asList(features));
            list.sort(null);

            Event.Builder builder = Event.newBuilder();
            builder.setEventTime(values[2]);
            builder.setCity(values[3]);
            builder.setVin(values[4]);
            builder.setYear(Integer.valueOf(values[6]));
            builder.setMake(values[7]);
            builder.setModel(values[8]);
            builder.setPrice(Double.valueOf(values[12]));
            builder.setMileage(Integer.valueOf(values[13]));
            builder.setFeatures(list);

            //Trim
            if (values[9].equals(""))
                builder.setTrim(null);
            else
                builder.setTrim(values[9]);

            //Event Type
            EventType et;
            switch(eventFields[0].toLowerCase()){
                case "visit":
                    et = EventType.VISIT;
                    break;
                case "change":
                    et = EventType.CHANGE;
                    break;
                case "show":
                    et = EventType.SHOW;
                    break;
                case "click":
                    et = EventType.CLICK;
                    break;
                case "display":
                    et = EventType.DISPLAY;
                    break;
                case "edit":
                    et = EventType.EDIT;
                    break;
                default:
                    et = EventType.SUBMIT;
            }
            builder.setEventType(et);

            //Event SubType
            EventSubtype est;
            switch(eventFields[1].toLowerCase()) {
                case "alternative":
                    est = EventSubtype.ALTERNATIVE;
                    break;
                case "badges":
                    est = EventSubtype.BADGES;
                    break;
                case "badge detail":
                    est = EventSubtype.BADGE_DETAIL;
                    break;
                case "contact banner":
                    est = EventSubtype.CONTACT_BANNER;
                    break;
                case "contact button":
                    est = EventSubtype.CONTACT_BUTTON;
                    break;
                case "contact form":
                    est = EventSubtype.CONTACT_FORM;
                    break;
                case "dealer phone":
                    est = EventSubtype.DEALER_PHONE;
                    break;
                case "features":
                    est = EventSubtype.FEATURES;
                    break;
                case "get directions":
                    est = EventSubtype.GET_DIRECTIONS;
                    break;
                case "market report":
                    est = EventSubtype.MARKET_REPORT;
                    break;
                case "photo modal":
                    est = EventSubtype.PHOTO_MODAL;
                    break;
                default:
                    est = EventSubtype.VEHICLE_HISTORY;
            }
            builder.setEventSubtype(est);

            //Vehicle Condition
            VehicleCondition vc;
            if (values[5].equals("New"))
                vc = VehicleCondition.New;
            else
                vc = VehicleCondition.Used;
            builder.setCondition(vc);

            //Body Style
            BodyStyle bs;
            switch (values[10]) {
                case "Chassis":
                    bs = BodyStyle.CHASSIS;
                    break;
                case "Convertible":
                    bs = BodyStyle.CONVERTIBLE;
                    break;
                case "Coupe":
                    bs = BodyStyle.COUPE;
                    break;
                case "Hatchback":
                    bs = BodyStyle.HATCHBACK;
                    break;
                case "Minivan":
                    bs = BodyStyle.MINIVAN;
                    break;
                case "Pickup":
                    bs = BodyStyle.PICKUP;
                    break;
                case "SUV":
                    bs = BodyStyle.SUV;
                    break;
                case "Sedan":
                    bs = BodyStyle.SEDAN;
                    break;
                case "Van":
                    bs = BodyStyle.VAN;
                    break;
                case "Wagon":
                    bs = BodyStyle.WAGON;
                    break;
                default:
                    bs = BodyStyle.SUV;
            }
            builder.setBodyStyle(bs);

            //Cab Style
            CabStyle cs = null;
            switch(values[11]) {
                case "Crew Cab":
                    cs = CabStyle.CREW_CAB;
                    break;
                case "Extended Cab":
                    cs = CabStyle.EXTENDED_CAB;
                    break;
                case "Regular Cab":
                    cs = CabStyle.REGULAR_CAB;
                    break;
                default:
                    cs = null;
            }
            builder.setCabStyle(cs);

            //Free Carfax Report
            if(values[14].equals("f"))
                builder.setFreeCarfaxReport(false);
            else
                builder.setFreeCarfaxReport(true);

            user.set(values[0]); //Set user_id as output key
            context.write(user, new AvroValue<Event>(builder.build()));
        }
    }

    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
	public static class SessionsReduce
			extends Reducer<Text, AvroValue<Event>,
			                AvroKey<CharSequence>, AvroValue<Session>> {

        static final Comparator<Event> TIMESTAMP_ORDER = new Comparator<Event>() {
            public int compare(Event o1, Event o2) {
                int res = o1.getEventTime().toString().compareTo(o2.getEventTime().toString());
                if (res == 0)
                    res = o1.getEventType().toString().compareTo(o2.getEventType().toString());
                return res;
            }
        };

        @Override
        public void reduce(Text key, Iterable<AvroValue<Event>> values, Context context)
                throws IOException, InterruptedException {
            Set<String> set = new HashSet<>();
            List<Event> list = new ArrayList<Event>();
            for (AvroValue<Event> event: values){
                if (!set.contains(event.datum().toString())) {
                    list.add(Event.newBuilder(event.datum()).build());
                    set.add(event.datum().toString());
                }
            }

            list.sort(TIMESTAMP_ORDER);
            Session.Builder builder = Session.newBuilder();
            builder.setUserId(key.toString());
            builder.setEvents(list);

            // Emit the total count for the word.
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(builder.build()));
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SessionsMR <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "SessionsMR");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(SessionsMR.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(SessionsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Event.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(SessionsReduce.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
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
        int res = ToolRunner.run(new SessionsMR(), args);
        System.exit(res);
    }
}
