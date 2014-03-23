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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to map the crucial values from the database input to the output
 *
 * @author Richárd Ernő Kiss
 */
public class DBToMapFileMapper extends Mapper<LongWritable, DBInputWritable, IntWritable, Text> {

    @Override
    protected void map(LongWritable id, DBInputWritable value, Context ctx) throws IOException, InterruptedException {
        ctx.write(new IntWritable(value.getId()), new Text(value.getCategory() + ReportGeneratorDriver.SEPARATOR + value.getPrice()));
    }
}
