/*
 * Copyright (C) 2014 Richárd Ernő Kiss
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package nl.sanoma.hdt.report.generator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Richárd Ernő Kiss
 */
public class ReportGeneratorMapperTest {
    
    public ReportGeneratorMapperTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of map method, of class ReportGeneratorMapper.
     */
    /*@Test
    public void testMap() throws Exception {
        System.out.println("map");
        LongWritable id = new LongWritable(1);
        Text value = new Text("2013-01-01þ10000þ1þ1þ1þ10");
        Mapper.Context ctx = mock(ReportGeneratorMapper.Context.class);;
        ReportGeneratorMapper instance = new ReportGeneratorMapper();
        instance.map(id, value, ctx);
        verify(ctx).write(new IntWritable(1), new Revenue("Q1",3.0));
    }*/


    /**
     * Test of determineQuarter method, of class ReportGeneratorMapper.
     */
    @Test
    public void testDetermineQuarter() {
        System.out.println("determineQuarter");
        String date = "1999-03-07";
        String expResult = "Q1";
        String result = ReportGeneratorMapper.determineQuarter(date);
        assertEquals(expResult, result);
    }
    
}
