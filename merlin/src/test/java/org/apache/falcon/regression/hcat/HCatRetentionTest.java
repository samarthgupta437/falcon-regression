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

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.enumsAndConstants.FEED_TYPE;
import org.apache.falcon.regression.core.enumsAndConstants.RETENTION_UNITS;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HCatRetentionTest extends BaseTestClass {

    static Logger logger = Logger.getLogger(HCatRetentionTest.class);

    private Bundle bundle;
    public static HCatClient cli;
    final String testDir = "/HCatRetentionTest/";
    final String baseTestHDFSDir = baseHDFSDir + testDir;
    final String dBName = "default";
    final ColoHelper cluster = servers.get(0);
    final FileSystem clusterFS = serverFS.get(0);
    final OozieClient clusterOC = serverOC.get(0);
    String tableName;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        HadoopUtil.createDir(baseTestHDFSDir, clusterFS);
        cli = cluster.getClusterHelper().getHCatClient();
        bundle = new Bundle(BundleUtil.getHCat2Bundle(), cluster);
        bundle.generateUniqueBundle();
        bundle.submitClusters(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws HCatException {
        bundle.deleteBundle(prism);
        cli.dropTable(dBName, tableName, true);
    }

    @Test(enabled = true, dataProvider = "loopBelow", timeOut = 900000, groups = "embedded")
    public void testHCatRetention(int retentionPeriod, RETENTION_UNITS retentionUnit,
                                  FEED_TYPE feedType) throws Exception {

        /*the hcatalog table that is created changes tablename characters to lowercase. So the
          name in the feed should be the same.*/
        tableName = String.format("testhcatretention_%s_%d", retentionUnit.getValue(),
            retentionPeriod);
        createPartitionedTable(cli, dBName, tableName, baseTestHDFSDir, feedType);
        FeedMerlin feedElement = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundle));
        feedElement.setTableValue(dBName, tableName, getFeedPathValue(feedType));
        feedElement
            .insertRetentionValueInFeed(retentionUnit.getValue() + "(" + retentionPeriod + ")");
        if (retentionPeriod <= 0) {
            AssertUtil.assertFailed(prism.getFeedHelper()
                .submitEntity(URLS.SUBMIT_URL, BundleUtil.getInputFeedFromBundle(bundle)));
        } else {
            final DateTime dataStartTime = new DateTime(
                feedElement.getClusters().getClusters().get(0).getValidity().getStart(),
                DateTimeZone.UTC).withSecondOfMinute(0);
            final DateTime dataEndTime = new DateTime(
                feedElement.getClusters().getClusters().get(0).getValidity().getEnd(),
                DateTimeZone.UTC).withSecondOfMinute(0);
            final List<DateTime> dataDates =
                TimeUtil.getDatesOnEitherSide(dataStartTime, dataEndTime, feedType);
            final List<String> dataDateStrings = TimeUtil.convertDatesToString(dataDates,
                TimeUtil.getFormatStringForFeedType(feedType));
            AssertUtil.checkForListSizes(dataDates, dataDateStrings);
            final List<String> dataFolders = HadoopUtil.createPeriodicDataset(dataDateStrings,
                OSUtil.OOZIE_EXAMPLE_INPUT_LATE_INPUT, clusterFS, baseTestHDFSDir);
            addPartitionsToExternalTable(cli, dBName, tableName, feedType, dataDates, dataFolders);
            List<String> initialData =
                getHadoopDataFromDir(cluster, baseTestHDFSDir, testDir, feedType);
            List<HCatPartition> initialPtnList = cli.getPartitions(dBName, tableName);
            AssertUtil.checkForListSizes(initialData, initialPtnList);

            AssertUtil.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedElement.toString()));
            final String bundleId = OozieUtil.getBundles(clusterOC, feedElement.getName(),
                ENTITY_TYPE.FEED).get(0);
            OozieUtil.waitForRetentionWorkflowToSucceed(bundleId, clusterOC);
            AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(URLS.SUSPEND_URL,
                feedElement.toString()));

            List<String> expectedOutput = getExpectedOutput(retentionPeriod, retentionUnit,
                feedType, new DateTime(DateTimeZone.UTC), initialData);
            List<String> finalData = getHadoopDataFromDir(cluster, baseTestHDFSDir, testDir,
                feedType);
            List<HCatPartition> finalPtnList = cli.getPartitions(dBName, tableName);

            logger.info("checking expectedOutput and finalPtnList");
            AssertUtil.checkForListSizes(expectedOutput, finalPtnList);
            logger.info("checking expectedOutput and finalData");
            AssertUtil.checkForListSizes(expectedOutput, finalData);
            logger.info("finalData = " + finalData);
            logger.info("expectedOutput = " + expectedOutput);
            Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),
                    expectedOutput.toArray(new String[expectedOutput.size()])),
                "expectedOutput and finalData don't match");
        }
    }

    private static List<String> getHadoopDataFromDir(ColoHelper helper, String hadoopPath,
                                                     String dir, FEED_TYPE feedType)
        throws IOException {
        List<String> finalResult = new ArrayList<String>();
        final int dirDepth = getDirDepthForFeedType(feedType);

        List<Path> results = HadoopUtil.getAllDirsRecursivelyHDFS(helper,
            new Path(hadoopPath), dirDepth);

        for (Path result : results) {
            int pathDepth = result.toString().split(dir)[1].split("/").length - 1;
            if (pathDepth == dirDepth) {
                finalResult.add(result.toString().split(dir)[1]);
            }
        }

        return finalResult;
    }

    /**
     * Get the expected output after retention is applied
     *
     * @param retentionPeriod retention period
     * @param retentionUnit   retention unit
     * @param feedType        feed type
     * @param endDateUTC      end date of retention
     * @param inputData       input data on which retention was applied
     * @return expected output of the retention
     */
    private static List<String> getExpectedOutput(int retentionPeriod,
                                                  RETENTION_UNITS retentionUnit,
                                                  FEED_TYPE feedType,
                                                  DateTime endDateUTC,
                                                  List<String> inputData) {
        DateTimeFormatter formatter =
            DateTimeFormat.forPattern(TimeUtil.getFormatStringForFeedType(feedType));
        List<String> finalData = new ArrayList<String>();

        //convert the end date to the same format
        final String endLimit =
            formatter.print(getEndLimit(retentionPeriod, retentionUnit, endDateUTC));
        //now to actually check!
        for (String testDate : inputData) {
            if (testDate.compareTo(endLimit) >= 0) {
                finalData.add(testDate);
            }
        }
        return finalData;
    }

    private static void createPartitionedTable(HCatClient client, String dbName, String tableName,
                                               String tableLoc, FEED_TYPE dataType)
        throws HCatException {
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();

        //client.dropDatabase("sample_db", true, HCatClient.DropDBMode.CASCADE);

        cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.STRING, "id comment"));
        cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING, "value comment"));

        switch (dataType) {
            case MINUTELY:
                ptnCols.add(
                    new HCatFieldSchema("minute", HCatFieldSchema.Type.STRING, "min prt"));
            case HOURLY:
                ptnCols.add(
                    new HCatFieldSchema("hour", HCatFieldSchema.Type.STRING, "hour prt"));
            case DAILY:
                ptnCols.add(new HCatFieldSchema("day", HCatFieldSchema.Type.STRING, "day prt"));
            case MONTHLY:
                ptnCols.add(
                    new HCatFieldSchema("month", HCatFieldSchema.Type.STRING, "month prt"));
            case YEARLY:
                ptnCols.add(
                    new HCatFieldSchema("year", HCatFieldSchema.Type.STRING, "year prt"));
            default:
                break;
        }
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
            .create(dbName, tableName, cols)
            .fileFormat("rcfile")
            .ifNotExists(true)
            .partCols(ptnCols)
            .isTableExternal(true)
            .location(tableLoc)
            .build();
        client.dropTable(dbName, tableName, true);
        client.createTable(tableDesc);
    }

    private static void addPartitionsToExternalTable(HCatClient client, String dbName,
                                                     String tableName, FEED_TYPE feedType,
                                                     List<DateTime> dataDates,
                                                     List<String> dataFolders)
        throws HCatException {
        //Adding specific partitions that map to an external location
        Map<String, String> ptn = new HashMap<String, String>();
        for (int i = 0; i < dataDates.size(); ++i) {
            final String dataFolder = dataFolders.get(i);
            final DateTime dataDate = dataDates.get(i);
            switch (feedType) {
                case MINUTELY:
                    ptn.put("minute", "" + dataDate.getMinuteOfHour());
                case HOURLY:
                    ptn.put("hour", "" + dataDate.getHourOfDay());
                case DAILY:
                    ptn.put("day", "" + dataDate.getDayOfMonth());
                case MONTHLY:
                    ptn.put("month", "" + dataDate.getMonthOfYear());
                case YEARLY:
                    ptn.put("year", "" + dataDate.getYear());
                    break;
                default:
                    Assert.fail("Unexpected feedType = " + feedType);
            }
            //Each HCat partition maps to a directory, not to a file
            HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName,
                tableName, dataFolder, ptn).build();
            client.addPartition(addPtn);
            ptn.clear();
        }
    }

    private static String getFeedPathValue(FEED_TYPE feedType) {
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

    private static DateTime getEndLimit(int time, RETENTION_UNITS interval,
                                        DateTime today) {
        switch (interval) {
            case MINUTES:
                return today.minusMinutes(time);
            case HOURS:
                return today.minusHours(time);
            case DAYS:
                return today.minusDays(time);
            case MONTHS:
                return today.minusMonths(time);
            case YEARS:
                return today.minusYears(time);
            default:
                Assert.fail("Unexpected value of interval: " + interval);
        }
        return null;
    }

    @DataProvider(name = "loopBelow")
    public Object[][] getTestData(Method m) throws Exception {
        RETENTION_UNITS[] units = new RETENTION_UNITS[]{RETENTION_UNITS.HOURS, RETENTION_UNITS.DAYS,
            RETENTION_UNITS.MONTHS};// "minutes","years",
        int[] periods = new int[]{7, 824, 43}; // a negative value like -4 should be covered
        // in validation scenarios.
        FEED_TYPE[] dataTypes =
            new FEED_TYPE[]{
                //disabling since falcon has support is for only for single hcat partition
                //FEED_TYPE.DAILY, FEED_TYPE.MINUTELY, FEED_TYPE.HOURLY, FEED_TYPE.MONTHLY,
                FEED_TYPE.YEARLY};
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
