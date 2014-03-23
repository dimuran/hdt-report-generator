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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Custom writable to pass data for the reducer
 *
 * @author Richárd Ernő Kiss
 */
public class ValueData implements Writable {

    private String quarter;
    private Double revenue;
    private String category;

    /**
     * Constructor.
     */
    public ValueData() {
    }

    public ValueData(String quarter, String category, Double revenue) {
        this.revenue = revenue;
        this.quarter = quarter;
        this.category = category;

    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append('{')
                .append(quarter)
                .append(',')
                .append(category)
                .append(',')
                .append(revenue)
                .append('}')
                .toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        quarter = WritableUtils.readString(in);
        category = WritableUtils.readString(in);
        revenue = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, quarter);
        WritableUtils.writeString(out, category);
        out.writeDouble(revenue);
    }

    public String getQuarter() {
        return quarter;
    }

    public void setQuarter(String quarter) {
        this.quarter = quarter;
    }

    public Double getRevenue() {
        return revenue;
    }

    public void setRevenue(Double revenue) {
        this.revenue = revenue;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

}
