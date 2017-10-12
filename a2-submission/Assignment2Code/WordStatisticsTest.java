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
 * Unit test for the WordStatistics map-reduce program.
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
//    private static final String TEST_2 = "for it for.[123]";

    @Test
    public void testMapClass() {
        WordStatisticsWritable out = new WordStatisticsWritable();
        out.setLongs(1L, 1L, 1L);
        out.setDoubles(0,0);

        mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
        mapDriver.withOutput(new Text(TEST_WORD.toLowerCase()), out);
        try {
            mapDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }

    @Test
    public void testReduceClass() {
        WordStatisticsWritable in = new WordStatisticsWritable();
        in.setLongs(1L, 1L, 1L);
        in.setDoubles(1.0,1.0);
        List<WordStatistics.WordStatisticsWritable> valueList = Lists.newArrayList(in);
        WordStatisticsWritable out = new WordStatisticsWritable();
        out.setLongs(1L, 1L, 1L);
        out.setDoubles(1.0,0);

        reduceDriver.withInput(new Text(TEST_WORD), valueList);
        reduceDriver.withOutput(new Text(TEST_WORD), out);

        try {
            reduceDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }
}
