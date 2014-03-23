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

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

/**
 * Mapper for joining the data with the metadata from distributed cache and
 * calculating the most popular product category and the revenue by quarter.
 * @author Richárd Ernő Kiss
 */
public class ReportGeneratorMapper extends Mapper<LongWritable, Text, KeyData, ValueData> {

    private MapFile.Reader metadataMapReader = null;
    private static final Pattern Q1 = Pattern.compile("-0[123]-");
    private static final Pattern Q2 = Pattern.compile("-0[456]-");
    private static final Pattern Q3 = Pattern.compile("-0[789]-");
    private static final Pattern Q4 = Pattern.compile("-1[012]-");

    /**
     * Creating a reader for the metadata MapFile.
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper.Context context) throws IOException,
            InterruptedException {

        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context
                .getConfiguration());
        //caching only one file
        FileSystem dfs = FileSystem.get(context.getConfiguration());
        metadataMapReader = new MapFile.Reader(dfs, cacheFilesLocal[0].toString() + "/part-m-00000/", context.getConfiguration());
    }

    /**
     * Parsing out the user id, quantity and date from the log data, looking up the
     * price and category from metadata, calculating the totalPrice using price from data
     * and quantity from metadata, and calculating the quarter by the date.
     *
     * @param id
     * @param value
     * @param ctx
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable id, Text value, Mapper.Context ctx) throws IOException, InterruptedException {

        String[] dataSplits = value.toString().split(ReportGeneratorDriver.SEPARATOR);
        //skip header
        if (!dataSplits[0].equals("Date")) {
            /*
             0 - date
             2 - product id
             3 - user id
             5 - quantity
             */

            //look up the metadata
            Text mapValue = new Text();
            metadataMapReader.get(new IntWritable(Integer.parseInt(dataSplits[2])), mapValue);
            String[] metaSplits = mapValue.toString().split(ReportGeneratorDriver.SEPARATOR);
            /*
             0 - category
             1 - price
             */
            
            double totalPrice = Double.parseDouble(metaSplits[1]) * Integer.parseInt(dataSplits[5]);
            String quarter = determineQuarter(dataSplits[0]);
            
            ctx.write(new KeyData(Integer.parseInt(dataSplits[3]),metaSplits[0]), new ValueData(quarter,metaSplits[0],totalPrice));
        }
    }

    /**
     * Closing the reader for the metadata MapFile.
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Mapper.Context context) throws IOException,
            InterruptedException {
        metadataMapReader.close();
    }

    /**
     * Function to determine the quarter of the year by the date.
     * 
     * @param date the date of the transaction
     * @return the quarter of the year
     */
    public static String determineQuarter(String date) {
        if (Q1.matcher(date).find()) {
            return "Q1";
        } else if (Q2.matcher(date).find()) {
            return "Q2";
        } else if (Q3.matcher(date).find()) {
            return "Q3";
        } else {
            return "Q4";
        }

    }

}