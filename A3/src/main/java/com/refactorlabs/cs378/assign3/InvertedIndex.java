package com.refactorlabs.cs378.assign3;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * @author Jorge Zapien-Diaz
 */
public class InvertedIndex extends Configured implements Tool {

	/**
	 * The Map class for Inverted Index.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the InvertedIndex example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text field = new Text();
		private Text messageID = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

            Set<String> important = new HashSet<>();
            important.add("To:");
            important.add("Cc:");
            important.add("Bcc:");

            // For each message, parse the fields and map the field:email
            // to the associated Message-ID
            while (tokenizer.hasMoreTokens()) {
                // Go to next Message-ID
                String s = tokenizer.nextToken();
                if (s.endsWith("Message-ID:")) {
                    s = tokenizer.nextToken();
                    messageID.set(s);
                    // Go through each field of message header
                    boolean doneFields = false;
                    while (tokenizer.hasMoreTokens() && !doneFields) {
                        String s2 = tokenizer.nextToken();
                        String s3 = "";
                        // To, Cc, Bcc may have more than one email
                        if (important.contains(s2)) {
                            // Go through each email
                            boolean doneEmails = false;
                            while (tokenizer.hasMoreTokens() && !doneEmails) {
                                s3 = tokenizer.nextToken();
                                if (s3.endsWith(","))
                                    s3 = s3.substring(0, s3.length() - 1);
                                if (s3.endsWith(".com")) {
                                    field.set(s2 + s3);
                                    context.write(field, messageID);
                                }
                                else
                                    doneEmails = true;
                            }
                        }
                        //From will always only have one email, so handle accordingly
                        else if (s2.equals("From:")) {
                            s3 = tokenizer.nextToken();
                            field.set(s2 + s3);
                            context.write(field, messageID);
                        }
                        // Bcc and X-From are always last, so break from field loop
                        else if (s2.equals("X-From:") || s3.startsWith("Bcc"))
                            doneFields = true;
                    }
                }
            }
		}
	}

	/**
	 * The Reduce class for Inverted Index.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the Inverted Index example.
	 */
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> list = new ArrayList<>();
			for(Text val : values) {
			    list.add(val.toString());
            }
            String s = String.join(",", list);

			Text out = new Text();
		    out.set(s);
		    context.write(key, out);
		}
	}

	/**
	 * The run method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileInputFormat.addInputPaths(job, appArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
	}

}
