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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer to calculate the most popular category and revenue by quarter for
 * each user
 *
 * @author Richárd Ernő Kiss
 */
public class ReportGeneratorReducer extends Reducer<KeyData, ValueData, NullWritable, Text> {

    private final Text joinedText = new Text();
    private final StringBuilder builder = new StringBuilder();
    private final NullWritable nullKey = NullWritable.get();

    int max_counter = 0;
    String popularCategory = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        context.write(nullKey, new Text("User ID\tMost Pop. Prod. Cat.\tRevenue Q1\tQ2\tQ3\tQ4"));
    }

    /**
     * The categories are sorted for each user so the reducer can calculate a
     * simple max for the popular category the revenue is a simple aggregation.
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(KeyData key, Iterable<ValueData> values, Context context) throws IOException, InterruptedException {
        builder.append(key.getUserId()).append("\t");
        int counter = 0;
        String category = "";
        double q1Revenue = 0.0;
        double q2Revenue = 0.0;
        double q3Revenue = 0.0;
        double q4Revenue = 0.0;

        for (ValueData value : values) {

            //revenue calculation
            switch (value.getQuarter()) {
                case "Q1":
                    q1Revenue += value.getRevenue();
                    break;
                case "Q2":
                    q2Revenue += value.getRevenue();
                    break;
                case "Q3":
                    q3Revenue += value.getRevenue();
                    break;
                case "Q4":
                    q4Revenue += value.getRevenue();
                    break;
            }

            //popular category calculation
            if (category.equals("")) {
                category = value.getCategory();
            }

            if (!category.equals(value.getCategory())) {
                if (counter > max_counter) {
                    max_counter = counter;
                    popularCategory = category;
                }
                category = value.getCategory();
                counter = 0;
            }
            counter++;
        }
        if (counter > max_counter) {
            max_counter = counter;
            popularCategory = category;
        }
        builder.append(popularCategory).append("\t\t\t").append(q1Revenue).append("\t").append(q2Revenue).append("\t").append(q3Revenue).append("\t").append(q4Revenue);
        joinedText.set(builder.toString());
        context.write(nullKey, joinedText);
        builder.setLength(0);
        max_counter = 0;
        popularCategory = "";
    }
}
