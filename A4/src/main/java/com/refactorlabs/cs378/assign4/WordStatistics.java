package com.refactorlabs.cs378.assign4;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * MapReduce program that performs word statistics.
 *
 * @author Jorge Zapien-Diaz (jaz747@cs.utexas.edu)
 */
public class WordStatistics extends Configured implements Tool {

	/**
	 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for word statistics
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			// Keep a count for each word
			Map<String, Long> wordMap = new TreeMap<String, Long>();

			// Go through each word(token) and map it
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				token = token.toLowerCase().replaceAll("^\\p{Punct}+|\\p{Punct}+$","");

				// Split tokens that are words paired with footnotes (e.g. "parry[270]")
				if (!token.startsWith("[") && token.endsWith("]")) {
					int index = token.indexOf('[');
					String token2 = token.substring(index);
					token = token.substring(0,index);
					mapAdd(wordMap, token2);
				}

				mapAdd(wordMap, token);
			}

			for (String k : wordMap.keySet()) {
				word.set(k);
				long val = wordMap.get(k);

				// Save Values from wordCount via a WordStatisticsData object
				WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
				builder.setTotalCount(val);
                builder.setDocumentCount(1L);
                builder.setSumOfSquares(val*val);
                builder.setMin(val);
                builder.setMax(val);
                builder.setMean(0);
                builder.setVariance(0);

				// Write to context
				context.write(word, new AvroValue<WordStatisticsData>(builder.build()));
			}
		}

		private void mapAdd(Map<String, Long> wordMap, String token) {
			if (!wordMap.containsKey(token))
				wordMap.put(token, 1L);
			else
				wordMap.put(token, wordMap.get(token) + 1);
		}
	}

	/**
	 * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for word statistics.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>, Text, AvroValue<WordStatisticsData>> {

		@Override
		public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
				throws IOException, InterruptedException {

			long wordCount = 0;
			long countSquared = 0;
			long docCount = 0;
			long min = Integer.MAX_VALUE;
			long max = 0;
			double mean;
			double variance;

			for (AvroValue<WordStatisticsData> ws: values) {
				wordCount += ws.datum().getTotalCount();
				countSquared += ws.datum().getSumOfSquares();
				docCount += ws.datum().getDocumentCount();
				min = Math.min(min, ws.datum().getMin());
                max = Math.max(max, ws.datum().getMax());
			}

            // Save Values from wordCount via a WordStatisticsData object
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();

			mean = getMean(wordCount, docCount);
			variance = getVariance(countSquared, docCount, mean);

            builder.setTotalCount(wordCount);
            builder.setSumOfSquares(countSquared);
            builder.setDocumentCount(docCount);
			builder.setMean(mean);
			builder.setVariance(variance);
			builder.setMax(max);
			builder.setMin(min);

			context.write(key, new AvroValue<WordStatisticsData>(builder.build()));
		}

		private double getMean(double count, double docCount){
            return count/docCount;
		}

		private double getVariance(double countSquared, double docCount, double mean) {
            return (countSquared / docCount) - (mean * mean);
		}
	}

//	/**
//	 * The WordStatisticsWritable class for word statistics.
//	 */
//	public static class WordStatisticsWritable implements Writable {
//
//		/*Instance Variables*/
//		private long docCount;
//		private long count;
//		private long countSquared;
//		private double mean;
//		private double variance;
//
//		public WordStatisticsWritable() {
//			this.mean = 0;
//			this.variance = 0;
//		}
//
//		public void setLongs(long count, long docCount) {
//			this.count = count;
//			this.docCount = docCount;
//			this.countSquared = countSquared;
//		}
//
//		public void setDoubles(double mean, double variance) {
//			this.mean = mean;
//			this.variance = variance;
//		}
//
//		public void write(DataOutput out) throws IOException {
//			// Write long Values
//			out.writeLong(docCount);
//			out.writeLong(count);
//			out.writeLong(countSquared);
//
//			// Write double Values
//			out.writeDouble(mean);
//			out.writeDouble(variance);
//		}
//
//		public void readFields(DataInput in) throws IOException {
//
//			// Process long Values
//			long valLong = in.readLong();
//			this.docCount = valLong;
//			valLong = in.readLong();
//			this.count = valLong;
//			valLong = in.readLong();
//			this.countSquared = valLong;
//
//			// Process double values
//			double valDouble = in.readDouble();
//			this.mean = valDouble;
//			this.variance = in.readDouble();
//		}
//
//		public void setMean(double mean) {
//			this.mean = mean;
//		}
//
//		public void setVariance(double variance) {
//			this.variance = variance;
//		}
//
//		public double getMean() { return mean; }
//
//		public double getVariance() { return variance; }
//
//		public long getCount() { return count; }
//
//		public long getDocCount() { return docCount; }
//
//		public long getCountSquared() { return countSquared; }
//
//		@Override
//		public String toString() {
//			return docCount + ", " + mean + ", " + variance;
//		}
//
//		public boolean equals(WordStatisticsWritable other) {
//			//If Equal
//			if (this.docCount == other.docCount
//					&& this.count == other.count
//					&& this.mean == other.mean
//					&& this.variance == other.variance)
//				return true;
//
//			//If Not Equal
//			return false;
//		}
//	}


	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordStatistics <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WordStatistics");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Specify the Map
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPaths(job, appArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new WordStatistics(), args);
		System.exit(res);
	}

}
