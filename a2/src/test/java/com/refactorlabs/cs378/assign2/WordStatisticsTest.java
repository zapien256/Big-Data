package com.refactorlabs.cs378.assign2;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import com.refactorlabs.cs378.assign2.WordStatistics.WordStatisticsWritable;

/**
 * Unit test for the WordCount map-reduce program.
 *
 * @author Jorge Zapien-Diaz
 */
public class WordStatisticsTest {

    MapDriver<LongWritable, Text, Text, WordStatisticsWritable> mapDriver;
    ReduceDriver<Text, WordStatisticsWritable, Text, WordStatisticsWritable> reduceDriver;

    @Before
    public void setup() {
        WordStatistics.MapClass mapper = new WordStatistics.MapClass();
        WordStatistics.ReduceClass reducer = new WordStatistics.ReduceClass();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    private static final String TEST_WORD = "Yadayada";

    @Test
    public void testMapClass() {
        mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
        mapDriver.withOutput(new Text(TEST_WORD), new WordStatisticsWritable());
        try {
            mapDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }

    @Test
    public void testReduceClass() {
        List<WordStatistics.WordStatisticsWritable> valueList =
                Lists.newArrayList(new WordStatisticsWritable());
        reduceDriver.withInput(new Text(TEST_WORD), valueList);
        reduceDriver.withOutput(new Text(TEST_WORD), new WordStatisticsWritable());
        try {
            reduceDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }
}