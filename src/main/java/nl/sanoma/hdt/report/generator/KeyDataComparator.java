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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparator to ensure that the date will be sorted by categories, so the
 * reducer can process it one after another
 *
 * @author Richárd Ernő Kiss
 */
public class KeyDataComparator extends WritableComparator {

    protected KeyDataComparator() {
        super(KeyData.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyData k1 = (KeyData) w1;
        KeyData k2 = (KeyData) w2;

        int result = k1.getUserId().compareTo(k2.getUserId());
        if (0 == result) {
            result = k1.getCategory().compareTo(k2.getCategory());
        }
        return result;
    }
}
