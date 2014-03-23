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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;


/**
 * Class to help read the values from database.
 *
 * @author Richárd Ernő Kiss
 */
public class DBInputWritable implements Writable, DBWritable {

    private int id;
    private String name;
    private String category;
    private double price;

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException //Resultset object represents the data returned from a SQL statement
    {
        id = rs.getInt(1);
        name = rs.getString(2);
        category = rs.getString(3);
        price = rs.getDouble(4);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setInt(1, id);
        ps.setString(2, name);
        ps.setString(3, category);
        ps.setDouble(4, price);
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCategory() {
        return category;
    }

    public double getPrice() {
        return price;
    }
    
}
