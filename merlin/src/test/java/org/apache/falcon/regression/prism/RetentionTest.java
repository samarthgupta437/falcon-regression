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

package org.apache.falcon.regression.prism;


import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

@Test(groups = "embedded")
public class RetentionTest extends BaseTestClass {
    private static final String TEST_FOLDERS = "testFolders/";
    String baseTestHDFSDir = baseHDFSDir + "/RetentionTest/";
    String testHDFSDir = baseTestHDFSDir + TEST_FOLDERS;
    static Logger logger = Logger.getLogger(RetentionTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws IOException, JAXBException {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.getBundleData("RetentionBundles")[0];
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].setInputFeedDataPath(testHDFSDir);
        bundles[0].generateUniqueBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        prism.getFeedHelper()
            .delete(URLS.DELETE_URL, BundleUtil.getInputFeedFromBundle(bundles[0]));
        verifyFeedDeletion(BundleUtil.getInputFeedFromBundle(bundles[0]));
        removeBundles();
    }

    @Test
    public void testRetentionWithEmptyDirectories() throws Exception {
        // test for https://issues.apache.org/jira/browse/FALCON-321
        testRetention("24", "hours", true, "daily", false);
    }

    @Test(groups = {"0.1", "0.2", "prism"}, dataProvider = "betterDP", priority = -1)
    public void testRetention(String period, String unit, boolean gaps, String dataType,
                              boolean withData) throws Exception {
        String inputFeed = setFeedPathValue(BundleUtil.getInputFeedFromBundle(bundles[0]),
            getFeedPathValue(dataType));
        inputFeed = insertRetentionValueInFeed(inputFeed, unit + "(" + period + ")");

        bundles[0].submitClusters(prism);

        final ServiceResponse response = prism.getFeedHelper()
            .submitEntity(URLS.SUBMIT_URL, inputFeed);
        if (Integer.parseInt(period) > 0) {
            AssertUtil.assertSucceeded(response);

            replenishData(dataType, gaps, withData);

            commonDataRetentionWorkflow(inputFeed, Integer.parseInt(period), unit);
        } else {
            AssertUtil.assertFailed(response);
        }
    }

    private String setFeedPathValue(String feed, String pathValue) {
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feed);
        for (Location location : feedObject.getLocations().getLocations()) {
            if (location.getType() == LocationType.DATA) {
                location.setPath(pathValue);
            }
        }
        return feedObject.toString();
    }

    private void replenishData(String dataType, boolean gap,
                               boolean withData) throws Exception {
        int skip = 0;

        if (gap) {
            Random r = new Random();
            skip = gaps[r.nextInt(gaps.length)];
        }

        if (dataType.equalsIgnoreCase("daily")) {
            replenishData(
                convertDatesToFolders(getDailyDatesOnEitherSide(36, skip), skip), withData);
        } else if (dataType.equalsIgnoreCase("yearly")) {
            replenishData(getYearlyDatesOnEitherSide(10, skip), withData);
        } else if (dataType.equalsIgnoreCase("monthly")) {
            replenishData(getMonthlyDatesOnEitherSide(30, skip), withData);
        }
    }

    private String getFeedPathValue(String dataType) {
        if (dataType.equalsIgnoreCase("monthly")) {
            return testHDFSDir + "${YEAR}/${MONTH}";
        }
        if (dataType.equalsIgnoreCase("daily")) {
            return testHDFSDir + "${YEAR}/${MONTH}/${DAY}/${HOUR}";
        }
        if (dataType.equalsIgnoreCase("yearly")) {
            return testHDFSDir + "${YEAR}";
        }
        return null;
    }

    private void commonDataRetentionWorkflow(String inputFeed, int time,
                                             String interval)
        throws OozieClientException, IOException, URISyntaxException, AuthenticationException {
        //get Data created in the cluster
        List<String> initialData =
            Util.getHadoopDataFromDir(cluster, inputFeed,
                testHDFSDir);

        cluster.getFeedHelper()
            .schedule(URLS.SCHEDULE_URL, inputFeed);
        logger.info(cluster.getClusterHelper().getActiveMQ());
        final String inputDataSetName = Util.readDatasetName(inputFeed);
        logger.info(inputDataSetName);
        Consumer consumer =
            new Consumer("FALCON." + inputDataSetName,
                cluster.getClusterHelper().getActiveMQ());
        consumer.start();

        DateTime currentTime = new DateTime(DateTimeZone.UTC);
        String bundleId = OozieUtil.getBundles(clusterOC,
            inputDataSetName, ENTITY_TYPE.FEED).get(0);

        List<String> workflows = OozieUtil.waitForRetentionWorkflowToSucceed(bundleId, clusterOC);
        logger.info("workflows: " + workflows);

        consumer.interrupt();

        logger.info("deleted data which has been received from messaging queue:");
        for (HashMap<String, String> data : consumer.getMessageData()) {
            logger.info("*************************************");
            for (String key : data.keySet()) {
                logger.info(key + "=" + data.get(key));
            }
            logger.info("*************************************");
        }
        if (consumer.getMessageData().isEmpty()) {
            logger.info("Message data was empty!");
        }
        //now look for cluster data
        List<String> finalData =
            Util.getHadoopDataFromDir(cluster, inputFeed,
                testHDFSDir);

        //now see if retention value was matched to as expected
        List<String> expectedOutput =
            filterDataOnRetention(inputFeed, time, interval,
                currentTime, initialData);

        logger.info("initial data in system was:");
        for (String line : initialData) {
            logger.info(line);
        }

        logger.info("system output is:");
        for (String line : finalData) {
            logger.info(line);
        }

        logger.info("actual output is:");
        for (String line : expectedOutput) {
            logger.info(line);
        }

        validateDataFromFeedQueue(
            inputDataSetName,
            consumer.getMessageData(), expectedOutput, initialData);

        Assert.assertEquals(finalData.size(), expectedOutput.size(),
            "sizes of outputs are different! please check");

        Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),
            expectedOutput.toArray(new String[expectedOutput.size()])));
    }

    private void replenishData(List<String> folderList, boolean uploadData)
        throws IOException {
        //purge data first
        HadoopUtil.deleteDirIfExists(testHDFSDir, clusterFS);

        folderList.add("somethingRandom");

        for (final String folder : folderList) {
            final String pathString = testHDFSDir + folder;
            logger.info(pathString);
            clusterFS.mkdirs(new Path(pathString));
            if (uploadData) {
                clusterFS.copyFromLocalFile(new Path(OSUtil.RESOURCES + "log_01.txt"),
                    new Path(pathString));
            }
        }
    }

    private void validateDataFromFeedQueue(String feedName,
                                           List<HashMap<String, String>> queueData,
                                           List<String> expectedOutput,
                                           List<String> input) throws OozieClientException {

        //just verify that each element in queue is same as deleted data!
        input.removeAll(expectedOutput);

        List<String> jobIds = OozieUtil.getCoordinatorJobs(cluster,
            OozieUtil.getBundles(clusterOC,
                feedName, ENTITY_TYPE.FEED).get(0)
        );

        //create queuedata folderList:
        List<String> deletedFolders = new ArrayList<String>();

        for (HashMap<String, String> data : queueData) {
            if (data != null) {
                Assert.assertEquals(data.get("entityName"), feedName);
                String[] splitData = data.get("feedInstancePaths").split(TEST_FOLDERS);
                deletedFolders.add(splitData[splitData.length - 1]);
                Assert.assertEquals(data.get("operation"), "DELETE");
                Assert.assertEquals(data.get("workflowId"), jobIds.get(0));

                //verify other data also
                Assert.assertEquals(data.get("topicName"), "FALCON." + feedName);
                Assert.assertEquals(data.get("brokerImplClass"),
                    "org.apache.activemq.ActiveMQConnectionFactory");
                Assert.assertEquals(data.get("status"), "SUCCEEDED");
                Assert.assertEquals(data.get("brokerUrl"),
                    cluster.getFeedHelper().getActiveMQ());

            }
        }

        //now make sure queueData and input lists are same
        Assert.assertEquals(deletedFolders.size(), input.size(),
            "Output size is different than expected!");
        Assert.assertTrue(Arrays.deepEquals(input.toArray(new String[input.size()]),
                deletedFolders.toArray(new String[deletedFolders.size()])),
            "It appears that the data that is received from queue and the data deleted are " +
                "not same!");
    }

    private static String insertRetentionValueInFeed(String feed, String retentionValue) {
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feed);

        //insert retentionclause
        feedObject.getClusters().getClusters().get(0).getRetention()
            .setLimit(new Frequency(retentionValue));

        for (org.apache.falcon.entity.v0.feed.Cluster cluster : feedObject
            .getClusters().getClusters()) {
            cluster.getRetention().setLimit(new Frequency(retentionValue));
        }

        return feedObject.toString();

    }

    private void verifyFeedDeletion(String feed)
        throws IOException {
        String directory = "/projects/ivory/staging/" + cluster.getFeedHelper().getServiceUser()
            + "/workflows/feed/" + Util.readDatasetName(feed);
        //make sure feed bundle is not there
        Assert.assertFalse(clusterFS.isDirectory(new Path(directory)),
            "Feed " + Util.readDatasetName(feed) + " did not have its bundle removed!!!!");
    }

    private static List<String> convertDatesToFolders(List<String> dateList, int skipInterval) {
        logger.info("converting dates to folders....");
        List<String> folderList = new ArrayList<String>();

        for (String date : dateList) {
            for (int i = 0; i < 24; i += skipInterval + 1) {
                if (i < 10) {
                    folderList.add(date + "/0" + i);
                } else {
                    folderList.add(date + "/" + i);
                }
            }
        }

        return folderList;
    }

    private static List<String> getDailyDatesOnEitherSide(int interval, int skip) {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd");

        DateTime today = new DateTime(DateTimeZone.UTC);
        logger.info("today is: " + today.toString());

        List<String> dates = new ArrayList<String>();
        dates.add(formatter.print(today));

        //first lets get all dates before today
        for (int backward = 1; backward <= interval; backward += skip + 1) {
            dates.add(formatter.print(today.minusDays(backward)));
        }

        //now the forward dates
        for (int i = 1; i <= interval; i += skip + 1) {
            dates.add(formatter.print(today.plusDays(i)));
        }

        return dates;
    }

    private static List<String> getYearlyDatesOnEitherSide(int interval, int skip) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy");
        DateTime today = new DateTime(DateTimeZone.UTC);
        logger.info("today is: " + today.toString());

        List<String> dates = new ArrayList<String>();
        dates.add(formatter.print(new LocalDate(today)));

        //first lets get all dates before today
        for (int backward = 1; backward <= interval; backward += skip + 1) {
            dates.add(formatter.print(new LocalDate(today.minusYears(backward))));
        }

        //now the forward dates
        for (int i = 1; i <= interval; i += skip + 1) {
            dates.add(formatter.print(new LocalDate(today.plusYears(i))));
        }

        return dates;
    }

    private static List<String> getMonthlyDatesOnEitherSide(int interval, int skip) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM");
        DateTime today = new DateTime(DateTimeZone.UTC);
        logger.info("today is: " + today.toString());

        List<String> dates = new ArrayList<String>();
        dates.add(formatter.print((today)));

        //first lets get all dates before today
        for (int backward = 1; backward <= interval; backward += skip + 1) {
            dates.add(formatter.print(new LocalDate(today.minusMonths(backward))));
        }

        //now the forward dates
        for (int i = 1; i <= interval; i += skip + 1) {
            dates.add(formatter.print(new LocalDate(today.plusMonths(i))));
        }

        return dates;
    }

    private List<String> filterDataOnRetention(String feed, int time, String interval,
                                               DateTime endDate,
                                               List<String> inputData) {
        String locationType = "";
        String appender = "";

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        List<String> finalData = new ArrayList<String>();

        //determine what kind of data is there in the feed!
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feed);

        for (Location location : feedObject.getLocations().getLocations()) {
            if (location.getType() == LocationType.DATA) {
                locationType = location.getPath();
            }
        }

        if (locationType.equalsIgnoreCase("") || locationType.equalsIgnoreCase(null)) {
            throw new TestNGException("location type was not mentioned in your feed!");
        }

        if (locationType.equalsIgnoreCase(testHDFSDir + "${YEAR}/${MONTH}")) {
            appender = "/01/00/01";
        } else if (locationType
            .equalsIgnoreCase(testHDFSDir + "${YEAR}/${MONTH}/${DAY}")) {
            appender = "/01"; //because we already take care of that!
        } else if (locationType
            .equalsIgnoreCase(testHDFSDir + "${YEAR}/${MONTH}/${DAY}/${HOUR}")) {
            appender = "/01";
        } else if (locationType.equalsIgnoreCase(testHDFSDir + "${YEAR}")) {
            appender = "/01/01/00/01";
        }

        //convert the start and end date boundaries to the same format


        //end date is today's date
        formatter.print(endDate);
        String startLimit = "";

        if (interval.equalsIgnoreCase("minutes")) {
            startLimit =
                formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusMinutes(time));
        } else if (interval.equalsIgnoreCase("hours")) {
            startLimit = formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusHours(time));
        } else if (interval.equalsIgnoreCase("days")) {
            startLimit = formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusDays(time));
        } else if (interval.equalsIgnoreCase("months")) {
            startLimit =
                formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusDays(31 * time));

        }


        //now to actually check!
        for (String testDate : inputData) {
            if (!testDate.equalsIgnoreCase("somethingRandom")) {
                if ((testDate + appender).compareTo(startLimit) > 0) {
                    finalData.add(testDate);
                }
            } else {
                finalData.add(testDate);
            }
        }

        return finalData;

    }

    final static int[] gaps = new int[]{2, 4, 5, 1};

    @DataProvider(name = "betterDP")
    public Object[][] getTestData(Method m) {
        String[] periods = new String[]{"0", "10080", "60", "8",
            "24"}; // a negative value like -4 should be covered in validation scenarios.
        String[] units = new String[]{"hours", "days"};// "minutes","hours","days",
        boolean[] gaps = new boolean[]{false, true};
        String[] dataTypes = new String[]{"daily", "yearly", "monthly"};
        Object[][] testData = new Object[periods.length * units.length *
            gaps.length * dataTypes.length][5];

        int i = 0;

        for (String unit : units) {
            for (String period : periods) {
                for (boolean gap : gaps) {
                    for (String dataType : dataTypes) {
                        testData[i][0] = period;
                        testData[i][1] = unit;
                        testData[i][2] = gap;
                        testData[i][3] = dataType;
                        testData[i][4] = true;
                        i++;
                    }
                }
            }
        }

        return testData;
    }

}
