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

package org.apache.falcon.regression.core.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.text.ParseException;


/*
all time / date related util methods for merlin . need to move methods from
instanceUtil to here , pending item.
 */

public class TimeUtil {
  public static String get20roundedTime(String oozieBaseTime) throws ParseException {
    DateTime startTime =
      new DateTime(InstanceUtil.oozieDateToDate(oozieBaseTime), DateTimeZone.UTC);

    if (startTime.getMinuteOfHour() < 20)
      startTime = startTime.minusMinutes(startTime.getMinuteOfHour());
    else if (startTime.getMinuteOfHour() < 40)
      startTime = startTime.minusMinutes(startTime.getMinuteOfHour()+20);
    else
      startTime = startTime.minusMinutes(startTime.getMinuteOfHour()+40);
    return InstanceUtil.dateToOozieDate(startTime.toDate());

  }
}
