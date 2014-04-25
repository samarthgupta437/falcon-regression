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

import com.jcraft.jsch.JSchException;
import org.apache.falcon.regression.core.enumsAndConstants.FEED_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/*
all time / date related util methods for merlin . need to move methods from
instanceUtil to here , pending item.
 */

public class TimeUtil {
    public static String get20roundedTime(String oozieBaseTime) throws ParseException {
        DateTime startTime =
                new DateTime(oozieDateToDate(oozieBaseTime), DateTimeZone.UTC);

        if (startTime.getMinuteOfHour() < 20)
            startTime = startTime.minusMinutes(startTime.getMinuteOfHour());
        else if (startTime.getMinuteOfHour() < 40)
            startTime = startTime.minusMinutes(startTime.getMinuteOfHour() + 20);
        else
            startTime = startTime.minusMinutes(startTime.getMinuteOfHour() + 40);
        return dateToOozieDate(startTime.toDate());

    }

    public static List<String> getMinuteDatesOnEitherSide(int interval, int minuteSkip) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        if (minuteSkip == 0) {
            minuteSkip = 1;
        }
        DateTime today = new DateTime(DateTimeZone.UTC);
        Util.logger.info("today is: " + today.toString());

        List<String> dates = new ArrayList<String>();
        dates.add(formatter.print(today));

        //first lets get all dates before today
        for (int backward = 1; backward <= interval; backward += minuteSkip) {
            dates.add(formatter.print(today.minusMinutes(backward)));
        }

        //now the forward dates
        for (int i = 0; i <= interval; i += minuteSkip) {
            dates.add(formatter.print(today.plusMinutes(i)));
        }

        return dates;
    }

    public static List<String> getMinuteDatesOnEitherSide(DateTime startDate, DateTime endDate,
                                                          int minuteSkip) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        formatter.withZoneUTC();
        Util.logger.info("generating data between " + formatter.print(startDate) + " and " +
                formatter.print(endDate));

        List<String> dates = new ArrayList<String>();


        while (!startDate.isAfter(endDate)) {
            dates.add(formatter.print(startDate.plusMinutes(minuteSkip)));
            if (minuteSkip == 0) {
                minuteSkip = 1;
            }
            startDate = startDate.plusMinutes(minuteSkip);
        }

        return dates;
    }

    public static List<String> getDatesOnEitherSide(DateTime startDate, DateTime endDate,
                                                    FEED_TYPE dataType) {
        int counter = 0, skip = 0;
        List<String> dates = new ArrayList<String>();

        while (!startDate.isAfter(endDate) && counter < 1000) {

            if (counter == 1 && skip == 0) {
                skip = 1;
            }

            switch (dataType) {
                case MINUTELY:
                    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
                    formatter.withZoneUTC();
                    dates.add(formatter.print(startDate.plusMinutes(skip)));
                    startDate = startDate.plusMinutes(skip);
                    break;

                case HOURLY:
                    formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH");
                    formatter.withZoneUTC();
                    dates.add(formatter.print(startDate.plusHours(skip)));
                    startDate = startDate.plusHours(skip);
                    break;

                case DAILY:
                    formatter = DateTimeFormat.forPattern("yyyy/MM/dd");
                    formatter.withZoneUTC();
                    dates.add(formatter.print(startDate.plusDays(skip)));
                    startDate = startDate.plusDays(skip);
                    break;

                case MONTHLY:
                    formatter = DateTimeFormat.forPattern("yyyy/MM");
                    formatter.withZoneUTC();
                    dates.add(formatter.print(startDate.plusMonths(skip)));
                    startDate = startDate.plusMonths(skip);
                    break;

                case YEARLY:
                    formatter = DateTimeFormat.forPattern("yyyy");
                    formatter.withZoneUTC();
                    dates.add(formatter.print(startDate.plusYears(skip)));
                    startDate = startDate.plusYears(skip);
            }//end of switch
            ++counter;
        }//end of while

        return dates;
    }

    public static String getTimeWrtSystemTime(int minutes) {

        DateTime jodaTime = new DateTime(DateTimeZone.UTC);
        if (minutes > 0)
            jodaTime = jodaTime.plusMinutes(minutes);
        else
            jodaTime = jodaTime.minusMinutes(-1 * minutes);

        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        DateTimeZone tz = DateTimeZone.getDefault();
        return fmt.print(tz.convertLocalToUTC(jodaTime.getMillis(), false));
    }

    public static String addMinsToTime(String time, int minutes) throws ParseException {

        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        DateTime jodaTime = fmt.parseDateTime(time);
        jodaTime = jodaTime.plusMinutes(minutes);
        return fmt.print(jodaTime);
    }

    public static DateTime oozieDateToDate(String time) throws ParseException {
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        fmt = fmt.withZoneUTC();
        return fmt.parseDateTime(time);
    }

    public static String dateToOozieDate(Date dt) throws ParseException {

        DateTime jodaTime = new DateTime(dt, DateTimeZone.UTC);
        InstanceUtil.logger.info("SystemTime: " + jodaTime);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        return fmt.print(jodaTime);
    }

    public static void sleepTill(PrismHelper prismHelper, String startTimeOfLateCoord)
    throws ParseException, IOException, JSchException {

        DateTime finalDate = new DateTime(oozieDateToDate(startTimeOfLateCoord));

        while (true) {
            DateTime sysDate = oozieDateToDate(getTimeWrtSystemTime(0));
            sysDate.withZoneRetainFields(DateTimeZone.UTC);
            InstanceUtil.logger.info("sysDate: " + sysDate + "  finalDate: " + finalDate);
            if (sysDate.compareTo(finalDate) > 0)
                break;

            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                InstanceUtil.logger.error(e.getMessage());
            }
        }

    }

    public static void createDataWithinDatesAndPrefix(ColoHelper colo, DateTime startDateJoda,
                                                      DateTime endDateJoda, String prefix,
                                                      int interval)
    throws IOException, InterruptedException {
        List<String> dataDates =
                getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, interval);

        if (!prefix.endsWith("/"))
            prefix = prefix + "/";

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        List<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) dataFolder.add(dataDate);

        InstanceUtil.putDataInFolders(colo, dataFolder, "");

    }

    public static List<String> createEmptyDirWithinDatesAndPrefix(ColoHelper colo,
                                                                  DateTime startDateJoda,
                                                                  DateTime endDateJoda,
                                                                  String prefix,
                                                                  int interval)
    throws IOException, InterruptedException {
        List<String> dataDates =
                getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, interval);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        List<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) dataFolder.add(dataDate);

        InstanceUtil.createHDFSFolders(colo, dataFolder);
        return dataFolder;
    }
}
