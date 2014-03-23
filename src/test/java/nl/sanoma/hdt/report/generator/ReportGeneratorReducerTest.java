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

import java.util.Arrays;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 * @author Richárd Ernő Kiss
 */
public class ReportGeneratorReducerTest {
    
    public ReportGeneratorReducerTest() {
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
     * Test of reduce method, of class ReportGeneratorReducer.
     */
    @Test
    public void testReduce() throws Exception {
        System.out.println("reduce");
        KeyData key = new KeyData(1, "candy");
        Iterable<ValueData> values = Arrays.asList(new ValueData("Q1","fruit",3.0), new ValueData("Q2","fruit",1.0), new ValueData("Q1","grocery",2.0), new ValueData("Q3","grocery",2.0), new ValueData("Q4","grocery",2.0));
        Reducer.Context context = mock(ReportGeneratorReducer.Context.class);;
        ReportGeneratorReducer instance = new ReportGeneratorReducer();
        instance.reduce(key, values, context);
        verify(context).write(NullWritable.get(), new Text("1\tgrocery\t\t\t5.0\t1.0\t2.0\t2.0"));

    }
    
}
