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
import org.apache.falcon.regression.core.enumsAndConstants.FeedType;
import org.apache.falcon.regression.core.enumsAndConstants.RetentionUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
    private static final Logger logger = Logger.getLogger(RetentionTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readRetentionBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].setInputFeedDataPath(testHDFSDir);
        bundles[0].generateUniqueBundle();
        bundles[0].submitClusters(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test
    public void testRetentionWithEmptyDirectories() throws Exception {
        // test for https://issues.apache.org/jira/browse/FALCON-321
        testRetention(24, RetentionUnit.HOURS, true, FeedType.DAILY, false);
    }

    @Test(groups = {"0.1", "0.2", "prism"}, dataProvider = "betterDP", priority = -1)
    public void testRetention(final int retentionPeriod, final RetentionUnit retentionUnit,
        final boolean gaps, final FeedType feedType, final boolean withData) throws Exception {
        String inputFeed = setFeedPathValue(bundles[0].getInputFeedFromBundle(),
            testHDFSDir + feedType.getPathValue());
        inputFeed = insertRetentionValueInFeed(inputFeed,
            retentionUnit.getValue() + "(" + retentionPeriod + ")");

        final ServiceResponse response = prism.getFeedHelper()
            .submitEntity(URLS.SUBMIT_URL, inputFeed);
        if (retentionPeriod > 0) {
            AssertUtil.assertSucceeded(response);

            replenishData(feedType, gaps, withData);

            commonDataRetentionWorkflow(inputFeed, retentionPeriod, retentionUnit);
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

    private void replenishData(FeedType feedType, boolean gap, boolean withData) throws Exception {
        int skip = 1;
        if (gap) {
            skip = gaps[new Random().nextInt(gaps.length)];
        }

        final DateTime today = new DateTime(DateTimeZone.UTC);
        final List<DateTime> times = TimeUtil.getDatesOnEitherSide(
            feedType.addTime(today, -36), feedType.addTime(today, 36), skip, feedType);
        final List<String> dataDates = TimeUtil.convertDatesToString(times, feedType.getFormatter());
        logger.info("dataDates = " + dataDates);

        HadoopUtil.replenishData(clusterFS, testHDFSDir, dataDates, withData);
    }

    private void commonDataRetentionWorkflow(String inputFeed, int time,
                                             RetentionUnit interval)
        throws OozieClientException, IOException, URISyntaxException, AuthenticationException {
        //get Data created in the cluster
        List<String> initialData =
            Util.getHadoopDataFromDir(clusterFS, inputFeed,
                testHDFSDir);

        cluster.getFeedHelper()
            .schedule(URLS.SCHEDULE_URL, inputFeed);
        logger.info(cluster.getClusterHelper().getActiveMQ());
        final String inputDataSetName = Util.readEntityName(inputFeed);
        logger.info(inputDataSetName);
        Consumer consumer =
            new Consumer("FALCON." + inputDataSetName,
                cluster.getClusterHelper().getActiveMQ());
        consumer.start();

        DateTime currentTime = new DateTime(DateTimeZone.UTC);
        String bundleId = OozieUtil.getBundles(clusterOC,
            inputDataSetName, EntityType.FEED).get(0);

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
            Util.getHadoopDataFromDir(clusterFS, inputFeed,
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

    private void validateDataFromFeedQueue(String feedName,
                                           List<HashMap<String, String>> queueData,
                                           List<String> expectedOutput,
                                           List<String> input) throws OozieClientException {

        //just verify that each element in queue is same as deleted data!
        input.removeAll(expectedOutput);

        List<String> jobIds = OozieUtil.getCoordinatorJobs(cluster,
            OozieUtil.getBundles(clusterOC,
                feedName, EntityType.FEED).get(0)
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

    private List<String> filterDataOnRetention(String feed, int time, RetentionUnit interval,
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

        if (interval == RetentionUnit.MINUTES) {
            startLimit =
                formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusMinutes(time));
        } else if (interval == RetentionUnit.HOURS) {
            startLimit = formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusHours(time));
        } else if (interval == RetentionUnit.DAYS) {
            startLimit = formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusDays(time));
        } else if (interval == RetentionUnit.MONTHS) {
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
        // a negative value like -4 should be covered in validation scenarios.
        int[] retentionPeriods = new int[]{0, 10080, 60, 8, 24};
        RetentionUnit[] retentionUnits = new RetentionUnit[]{RetentionUnit.HOURS,
            RetentionUnit.DAYS};// "minutes","hours", "days",
        boolean[] gaps = new boolean[]{false, true};
        FeedType[] feedTypes = new FeedType[]{FeedType.DAILY, FeedType.YEARLY, FeedType.MONTHLY};
        Object[][] testData = new Object[retentionPeriods.length * retentionUnits.length *
            gaps.length * feedTypes.length][5];

        int i = 0;

        for (RetentionUnit retentionUnit : retentionUnits) {
            for (int period : retentionPeriods) {
                for (boolean gap : gaps) {
                    for (FeedType feedType : feedTypes) {
                        testData[i][0] = period;
                        testData[i][1] = retentionUnit;
                        testData[i][2] = gap;
                        testData[i][3] = feedType;
                        testData[i][4] = true;
                        i++;
                    }
                }
            }
        }

        return testData;
    }

}
