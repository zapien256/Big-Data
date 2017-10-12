package com.refactorlabs.cs378.assign3;

import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for the WordCount map-reduce program.
 *
 * @author Jorge Zapien-Diaz
 */
public class InvertedIndexTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setup() {
        InvertedIndex.MapClass mapper = new InvertedIndex.MapClass();
        InvertedIndex.ReduceClass reducer = new InvertedIndex.ReduceClass();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

	@Test
	public void testReduceClass() {
        Text key = new Text("From:jane.tholt@enron.com");
		Text[] values = new Text[]{new Text("<23426663.1075857497542.JavaMail.evans@thyme>"),new Text("<23426663.1075857497542.JavaMail.evans@thyme>"),new Text("<14443098.1075857499469.JavaMail.evans@thyme>"),new Text("<6571521.1075857500090.JavaMail.evans@thyme>")};
		List<Text> valueList = new ArrayList<Text>();
		for (Text val: values)
		    valueList.add(val);
		Text out = new Text("<23426663.1075857497542.JavaMail.evans@thyme>,<23426663.1075857497542.JavaMail.evans@thyme>,<14443098.1075857499469.JavaMail.evans@thyme>,<6571521.1075857500090.JavaMail.evans@thyme>");

		reduceDriver.withInput(new Text("From:jane.tholt@enron.com"), valueList);
		reduceDriver.withOutput(new Text("From:jane.tholt@enron.com"), out);

		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
}