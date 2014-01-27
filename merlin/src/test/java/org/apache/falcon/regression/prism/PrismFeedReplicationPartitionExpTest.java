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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorAction;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class PrismFeedReplicationPartitionExpTest extends BaseMultiClusterTests {

    private Bundle bundle1, bundle2, bundle3;
    private String testDate = "/2012/10/01/12/";
    private String testBaseDir1 = baseHDFSDir + "/localDC/rc/billing";
    private String testBaseDir2 = baseHDFSDir + "/clusterPath/localDC/rc/billing";
    private String testBaseDir3 = baseHDFSDir + "/dataBillingRC/fetlrc/billing";
    private String testBaseDir4 = baseHDFSDir + "/data/fetlrc/billing";
    private String testDirWithDate = testBaseDir1 + testDate;
    private String dateTemplate = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    
    private String server1Colo = server1.getClusterHelper().getColo().split("=")[1];
    private String server2Colo = server2.getClusterHelper().getColo().split("=")[1];
    private String server3Colo = server3.getClusterHelper().getColo().split("=")[1];




// pt : partition in target
// ps: partition in source


    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        System.out.println("creating test data");

        HadoopUtil.createDir(testDirWithDate + "00/ua2/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "05/ua2/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "10/ua2/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "15/ua2/", server3FS);

        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "00/ua2/"), "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "05/ua2/"), "log_01.txt");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "10/ua2/"),
                "src/main/resources/gs1001.config.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "15/ua2/"),
                "src/main/resources/log4testng.properties");

        HadoopUtil.createDir(testDirWithDate + "00/ua1/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "05/ua1/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "10/ua1/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "15/ua1/", server3FS);

        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "00/ua1/"), "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "05/ua1/"), "log_01.txt");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "10/ua1/"),
                "src/main/resources/gs1001.config.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "15/ua1/"),
                "src/main/resources/log4testng.properties");

        HadoopUtil.createDir(testDirWithDate + "00/ua3/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "05/ua3/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "10/ua3/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "15/ua3/", server3FS);

        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "00/ua3/"), "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "05/ua3/"), "log_01.txt");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "10/ua3/"),
                "src/main/resources/gs1001.config.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "15/ua3/"),
                "src/main/resources/log4testng.properties");


        HadoopUtil.createDir(testBaseDir3 + testDate + "00/ua2/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "05/ua2/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "10/ua2/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "15/ua2/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "20/ua2/", server3FS);

        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "00/ua2/"), "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "05/ua2/"), "log_01.txt");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "10/ua2/"),
                "src/main/resources/gs1001.config.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "15/ua2/"),
                "src/main/resources/log4testng.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "20/ua2/"),
                "src/main/resources/log4testng.properties");


        HadoopUtil.createDir(testBaseDir3 + testDate + "00/ua1/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "05/ua1/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "10/ua1/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "15/ua1/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "20/ua1/", server3FS);


        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "00/ua1/"), "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "05/ua1/"), "log_01.txt");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "10/ua1/"),
                "src/main/resources/gs1001.config.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "15/ua1/"),
                "src/main/resources/log4testng.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "20/ua1/"),
                "src/main/resources/log4testng.properties");


        HadoopUtil.createDir(testBaseDir3 + testDate + "00/ua3/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "05/ua3/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "10/ua3/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "15/ua3/", server3FS);
        HadoopUtil.createDir(testBaseDir3 + testDate + "20/ua3/", server3FS);


        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "00/ua3/"), "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "05/ua3/"), "log_01.txt");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "10/ua3/"),
                "src/main/resources/gs1001.config.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "15/ua3/"),
                "src/main/resources/log4testng.properties");
        HadoopUtil.copyDataToFolder(server3, new Path(testBaseDir3 + testDate + "20/ua3/"),
                "src/main/resources/log4testng.properties");

        Util.print("completed creating test data");

    }

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = (Bundle) Bundle.readBundle("LocalDC_feedReplicaltion_BillingRC")[0][0];

        bundle1 = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());
        bundle2 = new Bundle(bundle, server2.getEnvFileName(), server2.getPrefix());
        bundle3 = new Bundle(bundle, server3.getEnvFileName(), server3.getPrefix());

        bundle1.generateUniqueBundle();
        bundle2.generateUniqueBundle();
        bundle3.generateUniqueBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {

        for (String dir : new String []{testBaseDir1, testBaseDir2, testBaseDir3, testBaseDir4}) {
            HadoopUtil.deleteDirIfExists(dir, server1FS);
            HadoopUtil.deleteDirIfExists(dir, server2FS);
        }
        prism.getFeedHelper().delete(URLS.DELETE_URL, bundle1.getDataSets().get(0));
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundle2.getClusters().get(0));
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundle1.getClusters().get(0));
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundle3.getClusters().get(0));
    }



    @Test(enabled = true)
    public void blankPartition() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally
        //partition is left blank

        bundle1.setCLusterColo(server1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundle2.setCLusterColo(server2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundle3.setCLusterColo(server3Colo);
        Util.print("cluster bundle3: " + bundle3.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle3.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE, "",
                testBaseDir1 + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET, "",
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE, "", null);

        Util.print("feed: " + feed);

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertFailed(r, "submit of feed should have fialed as the partiton in source is blank");
    }


    @Test(enabled = true)
    public void normalTest_1s1t1n_ps() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        // there are 1 source clusters 10.14.110.46
        //10.14.118.26 is the target
        //data should be replicated to 10.14.118.26 from 46

        // path for data in target cluster should also be customized
        Bundle.submitCluster(bundle1, bundle2, bundle3);
        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";


        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), null, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET, null,
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",testBaseDir1 + dateTemplate);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);
        Thread.sleep(15000);

        HadoopUtil.createDir(testDirWithDate + "00/ua3/", server3FS);
        HadoopUtil.createDir(testDirWithDate + "05/ua3/", server3FS);

        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "00/ua3/"),
                "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(server3, new Path(testDirWithDate + "05/ua3/"),
                "log_01.txt");

        InstanceUtil.waitTillInstanceReachState(server2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server2.getFeedHelper(), Util.readDatasetName(feed),
                "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server2.getFeedHelper(), Util.readDatasetName(feed),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server1.getFeedHelper(), Util.readDatasetName(feed),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server3.getFeedHelper(), Util.readDatasetName(feed),
                "RETENTION"), 1);


        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir2));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua2");


        List<Path> ua3ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "00/ua3/"));
        List<Path> ua3ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "05/ua3/"));

        List<Path> ua2ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir2 + testDate + "00"), "_SUCCESS");
        List<Path> ua2ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir2 + testDate + "05"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua3ReplicatedData00, ua2ReplicatedData00);
        AssertUtil.checkForPathsSizes(ua3ReplicatedData05, ua2ReplicatedData05);
    }


    @Test(enabled = true)
    public void normalTest_1s1t1n_pt() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally
        // path for data in target cluster should also be customized
        Bundle.submitCluster(bundle1, bundle2, bundle3);

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";


        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), null, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}", testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE, null,
                testBaseDir1 + dateTemplate);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        InstanceUtil.waitTillInstanceReachState(server2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(server2.getFeedHelper(), Util.readDatasetName(feed), "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(server2.getFeedHelper(), Util.readDatasetName(feed), "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(server1.getFeedHelper(), Util.readDatasetName(feed), "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(server3.getFeedHelper(), Util.readDatasetName(feed), "RETENTION"), 1);


        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil.getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir2));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua3ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "00/ua2/"));
        List<Path> ua3ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "05/ua2/"));

        List<Path> ua2ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir2 + testDate + "00"), "_SUCCESS");
        List<Path> ua2ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir2 + testDate + "05"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua3ReplicatedData00, ua2ReplicatedData00);
        AssertUtil.checkForPathsSizes(ua3ReplicatedData05, ua2ReplicatedData05);
    }


    @Test(enabled = true)
    public void normalTest_1s2t_pt() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //10.14.110.46 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on 10.14.117.33 and 10.14.118.26 as targets
        //ua3 is the source and ua1 and ua2 are target

        Bundle.submitCluster(bundle1, bundle2, bundle3);
        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed, testBaseDir3 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}", null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}", null);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE, null, null);


        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(15000);

        InstanceUtil.waitTillInstanceReachState(server1OC, Util.getFeedName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        InstanceUtil.waitTillInstanceReachState(server2OC, Util.getFeedName(feed), 3,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1,
                        new Path(testBaseDir3 + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2", "ua3");

        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2,
                        new Path(testBaseDir3 + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua1ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir3 + testDate + "00/"), "_SUCCESS");
        List<Path> ua1ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir3 + testDate + "10/"), "_SUCCESS");

        List<Path> ua2ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir3 + testDate + "10"), "_SUCCESS");
        List<Path> ua2ReplicatedData15 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir3 + testDate + "15"), "_SUCCESS");

        List<Path> ua3OriginalData00ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir3 + testDate + "00/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir3 + testDate + "10/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir3 + testDate + "10/ua2"), "_SUCCESS");
        List<Path> ua3OriginalData15ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir3 + testDate + "15/ua2"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua1ReplicatedData00, new ArrayList<Path>());
        AssertUtil.checkForPathsSizes(ua1ReplicatedData10, ua3OriginalData10ua1);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData15, ua3OriginalData15ua2);
    }

    @Test(enabled = true)
    public void normalTest_2s1t_pt() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        // there are 2 source clusters 10.14.110.46 and 10.14.117.33
        //10.14.118.26 is the target
        //data should be replicated to 10.14.118.26 from ua2 sub dir of 46 and 33
        // source cluster path in 33 should be mentioned in cluster definition
        // path for data in target cluster should also be customized

        bundle1.setCLusterColo(server1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundle2.setCLusterColo(server2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundle3.setCLusterColo(server3Colo);
        Util.print("cluster bundle3: " + bundle3.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle3.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE, null,
                testBaseDir1 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}",
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE, null, null);

        //clean target if old data exists
        Util.print("feed: " + feed);

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);
        Thread.sleep(15000);

        InstanceUtil.waitTillInstanceReachState(server1OC, Util.getFeedName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);
        InstanceUtil.waitTillInstanceReachState(server2OC, Util.getFeedName(feed), 3,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir4 + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2", "ua3");

        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir4 + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua1ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir4 + testDate + "00/"), "_SUCCESS");
        List<Path> ua1ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir4 + testDate + "05/"), "_SUCCESS");

        List<Path> ua2ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir4 + testDate + "10"), "_SUCCESS");
        List<Path> ua2ReplicatedData15 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir4 + testDate + "15"), "_SUCCESS");

        List<Path> ua3OriginalData00ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir4 + testDate + "00/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData05ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir4 + testDate + "05/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir4 + testDate + "10/ua2"), "_SUCCESS");
        List<Path> ua3OriginalData15ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir4 + testDate + "15/ua2"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua1ReplicatedData00, new ArrayList<Path>());
        AssertUtil.checkForPathsSizes(ua1ReplicatedData05, ua3OriginalData05ua1);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData15, ua3OriginalData15ua2);
    }


    @Test(enabled = true)
    public void normalTest_1s2t_ps() throws Exception {

        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //10.14.110.46 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on 10.14.117.33 and 10.14.118.26 as targets
        //ua3 is the source and ua1 and ua2 are target
        Bundle.submitCluster(bundle1, bundle2, bundle3);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed,
                testBaseDir1 + dateTemplate);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:11Z"),
                        XmlUtil.createRtention("days(10000000)", ActionType.DELETE),
                        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET, null,
                        testBaseDir1 + "/ua1" + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:26Z"),
                        XmlUtil.createRtention("days(10000000)", ActionType.DELETE),
                        Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET, null,
                        testBaseDir1 + "/ua2" + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", null);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(15000);

        InstanceUtil.waitTillInstanceReachState(server1OC, Util.getFeedName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
        InstanceUtil.waitTillInstanceReachState(server2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir1 + "/ua1" + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2");

        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir1 + "/ua2" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1");


        List<Path> ua1ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir1 + "/ua1" + testDate + "00/"), "_SUCCESS");
        List<Path> ua1ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir1 + "/ua1" + testDate + "05/"), "_SUCCESS");

        List<Path> ua2ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir1 + "/ua2" + testDate + "10"), "_SUCCESS");
        List<Path> ua2ReplicatedData15 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir1 + "/ua2" + testDate + "15"), "_SUCCESS");

        List<Path> ua3OriginalData05ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "05/ua3"), "_SUCCESS");
        List<Path> ua3OriginalData10ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "10/ua3"), "_SUCCESS");
        List<Path> ua3OriginalData15ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "15/ua3"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua1ReplicatedData00, new ArrayList<Path>());
        AssertUtil.checkForPathsSizes(ua1ReplicatedData05, ua3OriginalData05ua1);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData15, ua3OriginalData15ua2);

    }


    @Test(enabled = true)
    public void normalTest_2s1t_ps() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        // there are 2 source clusters 10.14.110.46 and 10.14.117.33
        //10.14.118.26 is the target
        //data should be replicated to 10.14.118.26 from ua2 sub dir of 46 and 33
        // source cluster path in 33 should be mentioned in cluster definition
        // path for data in target cluster should also be customized
        bundle1.setCLusterColo(server1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundle2.setCLusterColo(server2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundle3.setCLusterColo(server3Colo);
        Util.print("cluster bundle3: " + bundle3.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle3.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";

        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE,
                        "${cluster.colo}",
                        testBaseDir1 + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET, null,
                        testBaseDir2 + "/replicated" + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", testBaseDir1 + dateTemplate);

        Util.print("feed: " + feed);

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);
        Thread.sleep(15000);

        InstanceUtil.waitTillInstanceReachState(server2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil.getAllFilesRecursivelyHDFS(server2,
                new Path(testBaseDir2 + "/replicated" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua2");

        List<Path> ua2ReplicatedData00ua1 = HadoopUtil.getAllFilesRecursivelyHDFS(server2,
                new Path(testBaseDir2 + "/replicated" + testDate + "00/ua1"), "_SUCCESS");
        List<Path> ua2ReplicatedData05ua3 = HadoopUtil.getAllFilesRecursivelyHDFS(server2,
                new Path(testBaseDir2 + "/replicated" + testDate + "05/ua3/"), "_SUCCESS");


        List<Path> ua1OriginalData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir1 + testDate + "00/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server3, new Path(testDirWithDate + "05/ua1"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua2ReplicatedData00ua1, ua1OriginalData00);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData05ua3, ua3OriginalData05);
    }


    @Test(enabled = true)
    public void normalTest_1s2t_pst() throws Exception {


        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //10.14.110.46 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on 10.14.117.33 and 10.14.118.26 as targets
        //ua3 is the source and ua1 and ua2 are target
        bundle1.setCLusterColo(server1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundle2.setCLusterColo(server2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundle3.setCLusterColo(server3Colo);
        Util.print("cluster bundle3: " + bundle3.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle3.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";

        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed, testBaseDir1 + dateTemplate);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
                        "${cluster.colo}", testBaseDir1 + "/ua1" + dateTemplate + "/");
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET,
                        "${cluster.colo}", testBaseDir1 + "/ua2" + dateTemplate + "/");
        
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                , XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", null);

        Util.print("feed: " + feed);

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(15000);        
        InstanceUtil.waitTillInstanceReachState(server1OC, Util.getFeedName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);
        InstanceUtil.waitTillInstanceReachState(server2OC, Util.getFeedName(feed), 3,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir1 + "/ua1" + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2", "ua3");

        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir1 + "/ua2" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua1ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir1 + "/ua1" + testDate + "00/"), "_SUCCESS");
        List<Path> ua1ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server1, new Path(testBaseDir1 + "/ua1" + testDate + "10/"), "_SUCCESS");

        List<Path> ua2ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir1 + "/ua2" + testDate + "10"), "_SUCCESS");
        List<Path> ua2ReplicatedData15 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testBaseDir1 + "/ua2" + testDate + "15"), "_SUCCESS");

        List<Path> ua3OriginalData00ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testDirWithDate + "00/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testDirWithDate + "10/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testDirWithDate + "10/ua2"), "_SUCCESS");
        List<Path> ua3OriginalData15ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(server2, new Path(testDirWithDate + "15/ua2"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua1ReplicatedData00, new ArrayList<Path>());
        AssertUtil.checkForPathsSizes(ua1ReplicatedData10, ua3OriginalData10ua1);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData15, ua3OriginalData15ua2);
    }


    @Test(enabled = true)
    public void moreThanOneClusterWithSameNameDiffValidity() throws Exception {
        Bundle.submitCluster(bundle1, bundle2, bundle3);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";

        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE, "",
                testBaseDir1 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.TARGET, "",
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE, "", null);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertFailed(r, "is defined more than once for feed");
        Assert.assertTrue(r.getMessage().contains("is defined more than once for feed"));
    }
}
