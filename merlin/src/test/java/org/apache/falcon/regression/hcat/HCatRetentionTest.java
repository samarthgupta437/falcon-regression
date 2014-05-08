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

package org.apache.falcon.regression.hcat;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.enumsAndConstants.FEED_TYPE;
import org.apache.falcon.regression.core.enumsAndConstants.RETENTION_UNITS;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HCatRetentionTest extends BaseTestClass {

    static Logger logger = Logger.getLogger(HCatRetentionTest.class);

    private Bundle bundle;
    public static HCatClient cli;
    final String testDir = "/HCatRetentionTest/";
    final String baseTestHDFSDir = baseHDFSDir + testDir;
    final String dBName="default";
    final ColoHelper cluster = servers.get(0);
    final FileSystem clusterFS = serverFS.get(0);
    final OozieClient clusterOC = serverOC.get(0);

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        cli = HCatUtil.getHCatClient(cluster);
        bundle = new Bundle(BundleUtil.getHCat2Bundle(), cluster);
        HadoopUtil.createDir(baseTestHDFSDir, clusterFS);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws HCatException {
        bundle.deleteBundle(prism);
    }

    @Test(enabled = true, dataProvider = "loopBelow", timeOut = 900000, groups = "embedded")
    public void testHCatRetention(int retentionPeriod, RETENTION_UNITS retentionUnit,
                                  FEED_TYPE feedType) throws Exception {

        final String tableName = String.format("testhcatretention_%s_%d", retentionUnit.getValue(), retentionPeriod);
        /*the hcatalog table that is created changes tablename characters to lowercase. So the
          name in the feed should be the same.*/

        try{
            HCatUtil.createPartitionedTable(feedType, dBName, tableName, cli, baseTestHDFSDir);
            FeedMerlin feedElement = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundle));
            feedElement.setTableValue(getFeedPathValue(feedType),
                dBName, tableName);
            feedElement.insertRetentionValueInFeed(retentionUnit.getValue() + "(" + retentionPeriod + ")");
            bundle.getDataSets().remove(BundleUtil.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feedElement.toString());
            bundle.generateUniqueBundle();

            bundle.submitClusters(prism);

            if (retentionPeriod > 0) {
                AssertUtil.assertSucceeded(prism.getFeedHelper()
                        .submitEntity(URLS.SUBMIT_URL, BundleUtil.getInputFeedFromBundle(bundle)));

                feedElement = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundle));
                feedElement.generateData(cli, serverFS.get(0), "src/test/resources/OozieExampleInputData/lateData");
                check(feedType, retentionUnit, retentionPeriod, tableName);
            } else {
                AssertUtil.assertFailed(prism.getFeedHelper()
                    .submitEntity(URLS.SUBMIT_URL, BundleUtil.getInputFeedFromBundle(bundle)));
            }
        } finally {
            try {
                HCatUtil.deleteTable(cli, dBName, tableName);
            } catch(Exception e){
                logger.info("Exception during table delete:" + ExceptionUtils.getStackTrace(e));
            }
        }
    }

    public void check(FEED_TYPE feedType, RETENTION_UNITS retentionUnit, int retentionPeriod, String tableName)
            throws Exception {
        List<CoordinatorAction.Status> expectedStatus = new ArrayList<CoordinatorAction.Status>();
        expectedStatus.add(CoordinatorAction.Status.FAILED);
        expectedStatus.add(CoordinatorAction.Status.SUCCEEDED);
        expectedStatus.add(CoordinatorAction.Status.KILLED);
        expectedStatus.add(CoordinatorAction.Status.SUSPENDED);

        List<String> initialData =
                getHadoopDataFromDir(cluster, baseTestHDFSDir, testDir, feedType);

        List<HCatPartition> initialPtnList = cli.getPartitions(dBName, tableName);

        AssertUtil.checkForListSizes(initialData, initialPtnList);

        final String inputFeed = BundleUtil.getInputFeedFromBundle(bundle);
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, inputFeed));

        final String bundleId = OozieUtil.getBundles(clusterOC, Util.readDatasetName(inputFeed),
                ENTITY_TYPE.FEED).get(0);
        OozieUtil.waitForRetentionWorkflowToSucceed(bundleId, clusterOC);

        DateTime currentTimeUTC = new DateTime(DateTimeZone.UTC);

        List<String> finalData = getHadoopDataFromDir(cluster, baseTestHDFSDir, testDir, feedType);

        List<String> expectedOutput =
                filterDataOnRetentionHCat(retentionPeriod, retentionUnit,
                    feedType, currentTimeUTC, initialData);

        List<HCatPartition> finalPtnList = cli.getPartitions(dBName, tableName);

        logger.info("initial data in system was:");
        for (String line : initialData) {
            logger.info(line);
        }

        logger.info("system output is:");
        for (String line : finalData) {
            logger.info(line);
        }

        logger.info("expected output is:");
        for (String line : expectedOutput) {
            logger.info(line);
        }

        Assert.assertEquals(finalPtnList.size(), expectedOutput.size(),
                "sizes of hcat outputs are different! please check");

        //Checking if size of expected data and obtained data same
        Assert.assertEquals(finalData.size(), expectedOutput.size(),
                "sizes of hadoop outputs are different! please check");

        //Checking if the values are also the same
        Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),
                expectedOutput.toArray(new String[expectedOutput.size()])));

        //Checking if number of partitions left = size of remaining directories in HDFS
        Assert.assertEquals(finalData.size(), finalPtnList.size(),
                "sizes of outputs are different! please check");
    }

    private String getFeedPathValue(FEED_TYPE feedType) {
        switch (feedType) {
            case YEARLY:
                return "year=${YEAR}";
            case MONTHLY:
                return "year=${YEAR};month=${MONTH}";
            case DAILY:
                return "year=${YEAR};month=${MONTH};day=${DAY}";
            case HOURLY:
                return "year=${YEAR};month=${MONTH};day=${DAY};hour=${HOUR}";
            case MINUTELY:
                return "year=${YEAR};month=${MONTH};day=${DAY};hour=${HOUR};minute=${MINUTELY}";
            default:
                Assert.fail("Unexpected feedType=" + feedType);
        }
        return null;
    }

    public static List<String> getHadoopDataFromDir(ColoHelper helper, String hadoopPath,
                                                    String dir, FEED_TYPE feedType)
            throws IOException {
        List<String> finalResult = new ArrayList<String>();
        final int dirDepth = getDirDepthForFeedType(feedType);

        List<Path> results = HadoopUtil.getAllDirsRecursivelyHDFS(helper,
                new Path(hadoopPath), dirDepth);

        for (Path result : results) {
            int pathDepth = result.toString().split(dir)[1].split("/").length-1;
            if (pathDepth == dirDepth) {
                finalResult.add(result.toString().split(dir)[1]);
            }
        }

        return finalResult;
    }

    private static int getDirDepthForFeedType(FEED_TYPE feedType) {
        switch (feedType) {
            case MINUTELY:
                return 4;
            case HOURLY:
                return 3;
            case DAILY:
                return 2;
            case MONTHLY:
                return 1;
            case YEARLY:
                return 0;
            default:
                Assert.fail("Unexpected feedType=" + feedType);
        }
        return -1;
    }

    public static List<String> filterDataOnRetentionHCat(int retentionPeriod, RETENTION_UNITS retentionUnit,
                                                         FEED_TYPE feedType,
                                                         DateTime endDateUTC,
                                                         List<String> inputData) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        List<String> finalData = new ArrayList<String>();

        //determine what kind of data is there in the feed!

        final String appender;
        switch (feedType) {
            case YEARLY:
                appender = "/01/01/00/01";
                break;
            case MONTHLY:
                appender = "/01/00/01";
                break;
            case DAILY:
                appender = "/00/01"; //because we already take care of that!
                break;
            case HOURLY:
                appender = "/01";
                break;
            case MINUTELY:
                appender = "";
                break;
            default:
                appender = null;
                Assert.fail("Unexpected feedType = " + feedType);
        }

        //convert the start and end date boundaries to the same format
        //end date is today's date
        final String startLimit = getStartLimit(retentionPeriod, retentionUnit, formatter, endDateUTC);
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

    private static String getStartLimit(int time, RETENTION_UNITS interval,
                                        DateTimeFormatter formatter, DateTime today) {
        switch (interval) {
            case MINUTES:
                return formatter.print(today.minusMinutes(time));
            case HOURS:
                return formatter.print(today.minusHours(time));
            case DAYS:
                return formatter.print(today.minusDays(time));
            case MONTHS:
                return formatter.print(today.minusMonths(time));
            case YEARS:
                return formatter.print(today.minusYears(time));
            default:
                Assert.fail("Unexpected value of interval: " + interval);
        }
        return null;
    }

    @DataProvider(name = "loopBelow")
    public Object[][] getTestData(Method m) throws Exception {
        RETENTION_UNITS[] units = new RETENTION_UNITS[]{RETENTION_UNITS.HOURS, RETENTION_UNITS.DAYS, RETENTION_UNITS.MONTHS};// "minutes","years",
        int[] periods = new int[]{7, 824, 43}; // a negative value like -4 should be covered
        // in validation scenarios.
        FEED_TYPE[] dataTypes = new FEED_TYPE[]{FEED_TYPE.DAILY, FEED_TYPE.MINUTELY, FEED_TYPE.HOURLY, FEED_TYPE.MONTHLY, FEED_TYPE.YEARLY};
        Object[][] testData = new Object[units.length * periods.length * dataTypes.length][3];

        int i = 0;

        for (RETENTION_UNITS unit : units) {
            for (int period : periods) {
                for (FEED_TYPE dataType : dataTypes) {
                    testData[i][0] = period;
                    testData[i][1] = unit;
                    testData[i][2] = dataType;
                    i++;
                }
            }
        }
        return testData;
    }

}
