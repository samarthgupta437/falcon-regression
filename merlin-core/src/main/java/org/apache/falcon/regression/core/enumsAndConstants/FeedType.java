/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.core.enumsAndConstants;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Enum to represent different feed periodicity.
 */
public enum FeedType {
    MINUTELY("minutely", "yyyy/MM/dd/HH/mm", "${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}"),
    HOURLY("hourly",     "yyyy/MM/dd/HH",    "${YEAR}/${MONTH}/${DAY}/${HOUR}"),
    DAILY("daily",       "yyyy/MM/dd",       "${YEAR}/${MONTH}/${DAY}"),
    MONTHLY("monthly",   "yyyy/MM",          "${YEAR}/${MONTH}"),
    YEARLY("yearly",     "yyyy",             "${YEAR}");

    private final String value;
    private final String pathValue;
    private final DateTimeFormatter formatter;

    private FeedType(String value, String format, String pathValue) {
        this.value = value;
        formatter = DateTimeFormat.forPattern(format);
        this.pathValue = pathValue;
    }

    public String getValue() {
        return value;
    }

    public String getPathValue() {
        return pathValue;
    }

    public DateTimeFormatter getFormatter() {
        return formatter;
    }
}
