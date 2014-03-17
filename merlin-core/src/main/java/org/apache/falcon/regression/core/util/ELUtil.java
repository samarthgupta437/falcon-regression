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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.testng.Assert;
import org.testng.TestNGException;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class ELUtil {

    static Logger logger = Logger.getLogger(ELUtil.class);



    public static String testWith(PrismHelper prismHelper, ColoHelper server1, String feedStart, String feedEnd, String processStart,
                                  String processend,
                                  String startInstance, String endInstance, boolean isMatch) throws IOException, JAXBException, ParseException, URISyntaxException {
        Bundle bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());

        bundle.setFeedValidity(feedStart, feedEnd, Util.getInputFeedNameFromBundle(bundle));
        bundle.setProcessValidity(processStart, processend);
        try {

            bundle.setInvalidData();
            bundle.setDatasetInstances(startInstance, endInstance);
            String submitResponse = bundle.submitAndScheduleBundle(prismHelper);
            logger.info("processData in try is: " + bundle.getProcessData());
            Thread.sleep(45000);
            if (isMatch)
                getAndMatchDependencies(server1, bundle);

            return submitResponse;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            logger.info("deleting entity:");
            bundle.deleteBundle(prismHelper);
        }

    }


    public static String testWith(PrismHelper prismHelper, ColoHelper server1, String startInstance, String endInstance, boolean isMatch) throws IOException, JAXBException, URISyntaxException {
        Bundle bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());

        try {

            bundle.setInvalidData();
            bundle.setDatasetInstances(startInstance, endInstance);
            String submitResponse = bundle.submitAndScheduleBundle(prismHelper);
            Thread.sleep(45000);
            if (isMatch)
                getAndMatchDependencies(server1, bundle);

            return submitResponse;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            logger.info("deleting entity:");
            bundle.deleteBundle(prismHelper);
        }
    }


    public static void getAndMatchDependencies(PrismHelper prismHelper, Bundle bundle)
             {
        try {
            String coordID = Util.getBundles(prismHelper.getFeedHelper().getOozieClient(),
                    Util.getProcessName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            Util.print("coord id: " + coordID);
            Thread.sleep(30000);
            List<String> missingDependencies = Util.getMissingDependencies(prismHelper, coordID);
            for (String dependency : missingDependencies) {
                Util.print("dependency from job: " + dependency);
            }
            Date jobNominalTime = Util.getNominalTime(prismHelper, coordID);

            Calendar time = Calendar.getInstance();
            time.setTime(jobNominalTime);
            Util.print("nominalTime:" + jobNominalTime);
            Util.print("nominalTime in GNT string: " + jobNominalTime.toGMTString());

            TimeZone z = time.getTimeZone();
            int offset = z.getRawOffset();
            int offsetHrs = offset / 1000 / 60 / 60;
            int offsetMins = offset / 1000 / 60 % 60;

            System.out.println("offset: " + offsetHrs);
            System.out.println("offset: " + offsetMins);

            time.add(Calendar.HOUR_OF_DAY, (-offsetHrs));
            time.add(Calendar.MINUTE, (-offsetMins));

            logger.info("GMT Time: " + time.getTime());

            int frequency = bundle.getInitialDatasetFrequency();

            List<String> qaDependencyList =
                    getQADepedencyList(time, bundle.getStartInstanceProcess(time),
                            bundle.getEndInstanceProcess(time),
                            frequency, bundle);
            for (String qaDependency : qaDependencyList)
                Util.print("qa qaDependencyList: " + qaDependency);


            Assert.assertTrue(matchDependencies(missingDependencies, qaDependencyList));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
    }

    public static boolean matchDependencies(List<String> fromJob, List<String> QAList) {
        if (fromJob.size() != QAList.size())
            return false;

        for (int index = 0; index < fromJob.size(); index++) {
            if (!fromJob.get(index).contains(QAList.get(index)))
                return false;
        }

        return true;
    }


    public static List<String> getQADepedencyList(Calendar nominalTime, Date startRef,
                                                       Date endRef, int frequency,
                                                       Bundle bundle) throws JAXBException {

        Util.print("start ref:" + startRef);
        Util.print("end ref:" + endRef);

        Calendar initialTime = Calendar.getInstance();
        initialTime.setTime(startRef);
        Calendar finalTime = Calendar.getInstance();


        finalTime.setTime(endRef);
        String path = Util.getDatasetPath(bundle);

        TimeZone tz = TimeZone.getTimeZone("GMT");
        nominalTime.setTimeZone(tz);
        Util.print("nominalTime: " + initialTime.getTime());


        Util.print("finalTime: " + finalTime.getTime());

        List<String> returnList = new ArrayList<String>();

        while (!initialTime.getTime().equals(finalTime.getTime())) {
            Util.print("initialTime: " + initialTime.getTime());
            returnList.add(getPath(path, initialTime));
            initialTime.add(Calendar.MINUTE, frequency);
        }
        returnList.add(getPath(path, initialTime));
        Collections.reverse(returnList);
        return returnList;
    }


    public static String intToString(int num, int digits) {
        assert digits > 0 : "Invalid number of digits";

        // create variable length array of zeros
        char[] zeros = new char[digits];
        Arrays.fill(zeros, '0');
        // format number as String
        DecimalFormat df = new DecimalFormat(String.valueOf(zeros));

        return df.format(num);
    }

    public static String getPath(String path, Calendar time) {
        if (path.contains("${YEAR}")) {
            path = path.replaceAll("\\$\\{YEAR\\}", Integer.toString(time.get(Calendar.YEAR)));
        }

        if (path.contains("${MONTH}")) {
            path = path.replaceAll("\\$\\{MONTH\\}", intToString(time.get(Calendar.MONTH) + 1, 2));
        }


        if (path.contains("${DAY}")) {
            path = path.replaceAll("\\$\\{DAY\\}", intToString(time.get(Calendar.DAY_OF_MONTH), 2));
        }

        if (path.contains("${HOUR}")) {
            path = path.replaceAll("\\$\\{HOUR\\}", intToString(time.get(Calendar.HOUR_OF_DAY), 2));
        }

        if (path.contains("${MINUTE}")) {
            path = path.replaceAll("\\$\\{MINUTE\\}", intToString(time.get(Calendar.MINUTE), 2));
        }

        return path;
    }


    public static Date getMinutes(String expression, Calendar time) {
        int hr;
        int mins;
        int day;
        int month;

        Calendar cal = Calendar.getInstance();
        cal.setTime(time.getTime());
        if (expression.contains("now")) {
            hr = getInt(expression, 0);
            mins = getInt(expression, 1);
            cal.add(Calendar.HOUR, hr);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("today")) {
            hr = getInt(expression, 0);
            mins = getInt(expression, 1);
            cal.add(Calendar.HOUR, hr - (cal.get(Calendar.HOUR_OF_DAY)));
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("yesterday")) {
            hr = getInt(expression, 0);
            mins = getInt(expression, 1);
            cal.add(Calendar.HOUR, hr - (cal.get(Calendar.HOUR_OF_DAY)) - 24);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("currentMonth")) {
            day = getInt(expression, 0);
            hr = getInt(expression, 1);
            mins = getInt(expression, 2);
            cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), 1, 0, 0);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);

        } else if (expression.contains("lastMonth")) {
            day = getInt(expression, 0);
            hr = getInt(expression, 1);
            mins = getInt(expression, 2);
            cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) - 1, 1, 0, 0);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("currentYear")) {
            month = getInt(expression, 0);
            day = getInt(expression, 1);
            hr = getInt(expression, 2);
            mins = getInt(expression, 3);
            cal.set(cal.get(Calendar.YEAR), 1, 1, 0, 0);
            cal.add(Calendar.MONTH, month - 1);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("lastYear")) {
            month = getInt(expression, 0);
            day = getInt(expression, 1);
            hr = getInt(expression, 2);
            mins = getInt(expression, 3);
            cal.set(cal.get(Calendar.YEAR) - 1, 1, 1, 0, 0);
            cal.add(Calendar.MONTH, month - 1);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);
        }
        return cal.getTime();

    }

    private static int getInt(String expression, int position) {
        String numbers = expression.substring(expression.indexOf("(") + 1, expression.indexOf(")"));
        return Integer.parseInt(numbers.split(",")[position]);
    }
}


