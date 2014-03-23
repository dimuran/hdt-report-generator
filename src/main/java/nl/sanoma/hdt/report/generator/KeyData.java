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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Class to help sort by userId and by category
 * 
 * @author Richárd Ernő Kiss
 */
public class KeyData implements WritableComparable<KeyData> {

    private Integer userId;
    private String category;
    
    /**
     * Constructor.
     */
    public KeyData() {
    }

    public KeyData(Integer userId, String category) {
        this.userId = userId;
        this.category = category;
    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append('{')
                .append(userId)
                .append(',')
                .append(category)
                .append('}')
                .toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readInt();
        category = WritableUtils.readString(in);
        
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userId);
        WritableUtils.writeString(out, category);
        
    }

    @Override
    public int compareTo(KeyData o) {
        int result = userId.compareTo(o.userId);
        if (0 == result) {
                result = category.compareTo(o.category);
        }
        
        return result;
    }


    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }
    
    
}

