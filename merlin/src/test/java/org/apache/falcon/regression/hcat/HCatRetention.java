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
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.*;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.*;
import org.testng.log4testng.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class HCatRetention extends BaseTestClass {

    static Logger logger = Logger.getLogger(HCatRetention.class);

    private Bundle bundle;
    public static HCatClient cli;
    String testDir = "/HCatRetention/";
    String baseTestHDFSDir = baseHDFSDir + testDir;
    String dBName="default";
    String tableName="mytable";

    public HCatRetention() throws IOException {
        super();
        cli=HCatUtil.getHCatClient(servers.get(0));
    }

    @Test(enabled = true, dataProvider = "loopBelow")
    public void testHCatRetention(Bundle b, String period, String unit,String dataType) throws Exception {

        HCatUtil.createPartitionedTable(dataType, dBName, tableName, cli, baseTestHDFSDir);
        bundle = new Bundle(b, servers.get(0));
        int p= Integer.parseInt(period);
        displayDetails(period, unit, dataType);

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        String feed = setTableValue(Util.getInputFeedFromBundle(bundle), getFeedPathValue(dataType));
        feed = insertRetentionValueInFeed(feed, unit + "(" + period + ")");
        bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
        bundle.getDataSets().add(feed);
        bundle.generateUniqueBundle();

        bundle.submitClusters(prism);

        if (p > 0) {
            Util.assertSucceeded(prism.getFeedHelper()
                    .submitEntity(URLS.SUBMIT_URL, Util.getInputFeedFromBundle(bundle)));

            FeedMerlin feedElement = new FeedMerlin(Util.getInputFeedFromBundle(bundle));
            feedElement.generateData(cli, serverFS.get(0));

            check(dataType, unit, p);
        } else {
            Util.assertFailed(prism.getFeedHelper()
                    .submitEntity(URLS.SUBMIT_URL, Util.getInputFeedFromBundle(bundle)));
        }
    }

    public void check(String dataType, String unit, int period){
        try{

            List<String> initialData = getHadoopDataFromDir(servers.get(0), baseTestHDFSDir, "/HCatRetention/", dataType);

            //List<HCatPartition> initialPtnList = cli.getPartitions(dBName, tableName);

            Util.assertSucceeded(prism.getFeedHelper()
                    .schedule(URLS.SCHEDULE_URL, Util.getInputFeedFromBundle(bundle)));
            Thread.sleep(20000);

            DateTime currentTime = new DateTime(DateTimeZone.UTC);

            //List<String> finalData =
            //  Util.getHadoopDataFromDir2(servers.get(0), baseTestHDFSDir, "/HCatRetention/");

            List<String> expectedOutput =
                    Util.filterDataOnRetentionHCat(period, unit, dataType,
                            currentTime, initialData);

            List<HCatPartition> finalPtnList = cli.getPartitions(dBName,tableName);

            logger.info("initial data in system was:");
            for (String line : initialData) {
                logger.info(line);
            }

            logger.info("system output is:");
            for (HCatPartition line : finalPtnList) {
                logger.info(line);
            }

            logger.info("expected output is:");
            for (String line : expectedOutput) {
                logger.info(line);
            }

            Assert.assertEquals(expectedOutput.size(), finalPtnList.size(),
                    "sizes of hcat outputs are different! please check");

        }catch(Exception e){
            e.printStackTrace();
        }
        //Checking if size of expected data and obtained data same
        // Assert.assertEquals(finalData.size(), expectedOutput.size(),
        //     "sizes of hadoop outputs are different! please check");

        //Checking if the values are also the same
        //  Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),
        //     expectedOutput.toArray(new String[expectedOutput.size()])));

        //Checking if number of partitions left = size of remaining directories in HDFS
        //List<HCatPartition> finalPtnList = cli.getPartitions("default","mytable");
        //Assert.assertEquals(finalData.size(), finalPtnList.size(),
        //   "sizes of outputs are different! please check");

    }

    private void displayDetails(String period, String unit, String dataType)
            throws Exception {
        System.out.println("***********************************************");
        System.out.println("executing for:");
        System.out.println(unit + "(" + period + ")");
        System.out.println("dataType=" + dataType);
        System.out.println("***********************************************");
    }

    private String getFeedPathValue(String dataType) throws Exception {
        if (dataType.equalsIgnoreCase("monthly")) {
            return "year=${YEAR};month=${MONTH}";
        }
        if (dataType.equalsIgnoreCase("daily")) {
            return "year=${YEAR};month=${MONTH};day=${DAY}";
        }
        if (dataType.equalsIgnoreCase("hourly")) {
            return "year=${YEAR};month=${MONTH};day=${DAY};hour=${HOUR}";
        }
        if (dataType.equalsIgnoreCase("minutely")) {
            return "year=${YEAR};month=${MONTH};day=${DAY};hour=${HOUR};minute=${MINUTELY}";
        }
        if (dataType.equalsIgnoreCase("yearly")) {
            return "year=${YEAR}";
        }
        return null;
    }

    private static String insertRetentionValueInFeed(String feed, String retentionValue)
            throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Feed.class);
        Unmarshaller um = context.createUnmarshaller();
        Feed feedObject = (Feed) um.unmarshal(new StringReader(feed));

        //insert retentionclause
        feedObject.getClusters().getCluster().get(0).getRetention()
                .setLimit(new Frequency(retentionValue));

        for (org.apache.falcon.regression.core.generated.feed.Cluster cluster : feedObject
                .getClusters().getCluster()) {
            cluster.getRetention().setLimit(new Frequency(retentionValue));
        }

        StringWriter writer = new StringWriter();
        Marshaller m = context.createMarshaller();
        m.marshal(feedObject, writer);

        return writer.toString();

    }

    public static List<String> getHadoopDataFromDir(ColoHelper helper, String hadoopPath, String dir, String dataType)
            throws JAXBException, IOException {
        List<String> finalResult = new ArrayList<String>();
        int depth=0;

        if (dataType.equalsIgnoreCase("minutely")){
            depth=4;
        }
        else if (dataType.equalsIgnoreCase("hourly")){
            depth=3;
        }
        else if (dataType.equalsIgnoreCase("daily")){
            depth=2;
        }
        else if (dataType.equalsIgnoreCase("monthly")){
            depth=1;
        }
        else if (dataType.equalsIgnoreCase("yearly")){
            depth=0;
        }

        List<Path> results = HadoopUtil.getAllDirsRecursivelyHDFS(helper,
                new Path(hadoopPath), depth);

        for (Path result : results) {
            int pathDepth = result.toString().split(dir)[1].split("/").length-1;
            if (pathDepth == depth) {
                finalResult.add(result.toString().split(dir)[1]);
            }
        }

        return finalResult;
    }

    private String setTableValue(String feed, String pathValue) throws Exception {
        JAXBContext feedContext = JAXBContext.newInstance(Feed.class);
        Feed feedObject = (Feed) feedContext.createUnmarshaller().unmarshal(new StringReader(feed));

        feedObject.getTable().setUri("catalog:"+dBName+":"+tableName+"#"+pathValue);
        //set the value
        StringWriter feedWriter = new StringWriter();
        feedContext.createMarshaller().marshal(feedObject, feedWriter);
        return feedWriter.toString();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()throws Exception {

        bundle.deleteBundle(prism);
        Util.HDFSCleanup(serverFS.get(0), baseHDFSDir);
        HCatUtil.deleteTable(cli, dBName,tableName);
    }

    @DataProvider(name = "loopBelow")
    public Object[][] getTestData(Method m) throws Exception {
        Bundle[] bundles = Util.getBundleData("hcat_2");
        String[] units = new String[]{"months", "days", "minutes"};// "minutes","hours","days",
        String[] periods = new String[]{"6","824","43"}; // a negative value like -4 should be covered in validation scenarios.
        String[] dataTypes = new String[]{"monthly","hourly", "daily", "yearly", "monthly"};
        Object[][] testData = new Object[bundles.length * units.length * periods.length * dataTypes.length][4];

        int i = 0;

        for (Bundle bundle : bundles) {
            for (String unit : units) {
                for (String period : periods) {
                    for (String dataType : dataTypes) {
                        testData[i][0] = bundle;
                        testData[i][1] = period;
                        testData[i][2] = unit;
                        testData[i][3] = dataType;
                        i++;
                    }
                }
            }
        }
        return testData;
    }

}