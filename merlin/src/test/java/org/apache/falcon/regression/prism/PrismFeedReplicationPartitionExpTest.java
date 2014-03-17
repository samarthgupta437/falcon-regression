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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "distributed")
public class PrismFeedReplicationPartitionExpTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster1FS = serverFS.get(0);
    FileSystem cluster2FS = serverFS.get(1);
    FileSystem cluster3FS = serverFS.get(2);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    private String testDate = "/2012/10/01/12/";
    private String testBaseDir1 = baseHDFSDir + "/localDC/rc/billing";
    private String testBaseDir2 = baseHDFSDir + "/clusterPath/localDC/rc/billing";
    private String testBaseDir3 = baseHDFSDir + "/dataBillingRC/fetlrc/billing";
    private String testBaseDir4 = baseHDFSDir + "/data/fetlrc/billing";
    private String testDirWithDate = testBaseDir1 + testDate;
    private String dateTemplate = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String testFile1 =
      "src/test/resources/ReplicationResources/feed-s4Replication.xml" ;
    private String testFile2 =  "src/test/resources/ReplicationResources/id.pig" ;
    private String testFile3 =
      "src/test/resources/ReplicationResources/cluster-0.1.xml" ;
    private String testFile4 =
      "src/test/resources/ReplicationResources/log4testng.properties" ;
  
    public PrismFeedReplicationPartitionExpTest(){
        super();
        cluster1 = servers.get(0);
        cluster2 = servers.get(1);
        cluster3 = servers.get(2);
        cluster1FS = serverFS.get(0);
        cluster2FS = serverFS.get(1);
        cluster3FS = serverFS.get(2);
        cluster1OC = serverOC.get(0);
        cluster2OC = serverOC.get(1);
    }


// pt : partition in target
// ps: partition in source


    private void uploadDataToServer3(String location, String fileName) throws IOException,
    InterruptedException {
        HadoopUtil.createDir(location, cluster3FS);
        HadoopUtil.copyDataToFolder(cluster3, new Path(location), fileName);
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        System.out.println("creating test data");

        uploadDataToServer3(testDirWithDate + "00/ua2/", testFile1);
        uploadDataToServer3(testDirWithDate + "05/ua2/", testFile2);
        uploadDataToServer3(testDirWithDate + "10/ua2/", testFile3);
        uploadDataToServer3(testDirWithDate + "15/ua2/",
                testFile4);

        uploadDataToServer3(testDirWithDate + "00/ua1/", testFile1);
        uploadDataToServer3(testDirWithDate + "05/ua1/", testFile2);
        uploadDataToServer3(testDirWithDate + "10/ua1/",
                testFile3);
        uploadDataToServer3(testDirWithDate + "15/ua1/",
                "src/main/resources/log4testng.properties");

        uploadDataToServer3(testDirWithDate + "00/ua3/", testFile1);
        uploadDataToServer3(testDirWithDate + "05/ua3/", testFile2);
        uploadDataToServer3(testDirWithDate + "10/ua3/",
                testFile3);
        uploadDataToServer3(testDirWithDate + "15/ua3/",
                testFile4);


        uploadDataToServer3(testBaseDir3 + testDate + "00/ua2/", testFile1);
        uploadDataToServer3(testBaseDir3 + testDate + "05/ua2/", testFile2);
        uploadDataToServer3(testBaseDir3 + testDate + "10/ua2/",
                testFile3);
        uploadDataToServer3(testBaseDir3 + testDate + "15/ua2/",
                testFile4);
        uploadDataToServer3(testBaseDir3 + testDate + "20/ua2/",
                testFile4);


        uploadDataToServer3(testBaseDir3 + testDate + "00/ua1/", testFile1);
        uploadDataToServer3(testBaseDir3 + testDate + "05/ua1/", testFile2);
        uploadDataToServer3(testBaseDir3 + testDate + "10/ua1/",
                testFile3);
        uploadDataToServer3(testBaseDir3 + testDate + "15/ua1/",
                testFile4);
        uploadDataToServer3(testBaseDir3 + testDate + "20/ua1/",
                testFile4);


        uploadDataToServer3(testBaseDir3 + testDate + "00/ua3/", testFile1);
        uploadDataToServer3(testBaseDir3 + testDate + "05/ua3/", testFile2);
        uploadDataToServer3(testBaseDir3 + testDate + "10/ua3/",
                testFile3);
        uploadDataToServer3(testBaseDir3 + testDate + "15/ua3/",
                testFile4);
        uploadDataToServer3(testBaseDir3 + testDate + "20/ua3/",
                testFile4);

        Util.print("completed creating test data");

    }

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = (Bundle) Bundle.readBundle("LocalDC_feedReplicaltion_BillingRC")[0][0];

        bundles[0] = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
        bundles[1] = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
        bundles[2] = new Bundle(bundle, cluster3.getEnvFileName(), cluster3.getPrefix());

        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();
        bundles[2].generateUniqueBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        for (String dir : new String []{testBaseDir1, testBaseDir2, testBaseDir3, testBaseDir4}) {
            HadoopUtil.deleteDirIfExists(dir, cluster1FS);
            HadoopUtil.deleteDirIfExists(dir, cluster2FS);
        }
        removeBundles();
    }



    @Test(enabled = true)
    public void blankPartition() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally
        //partition is left blank

      Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

      String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, "",
                testBaseDir1 + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, "",
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE, "", null);

        Util.print("feed: " + feed);

      ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertFailed(r, "submit of feed should have fialed as the partiton in source is blank");
    }


    @Test(enabled = true)
    public void normalTest_1s1t1n_ps() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        // there are 1 source clusters cluster3
        //cluster2 is the target
        //data should be replicated to cluster2 from cluster3

        // path for data in target cluster should also be customized
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);
        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), null, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",testBaseDir1 + dateTemplate);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);
        Thread.sleep(15000);

        HadoopUtil.createDir(testDirWithDate + "00/ua3/", cluster3FS);
        HadoopUtil.createDir(testDirWithDate + "05/ua3/", cluster3FS);

        HadoopUtil.copyDataToFolder(cluster3, new Path(testDirWithDate + "00/ua3/"),
                "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(cluster3, new Path(testDirWithDate + "05/ua3/"),
                "log_01.txt");

        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feed),
                "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feed),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feed),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feed),
                "RETENTION"), 1);


        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir2));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua2");


        List<Path> ua3ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "00/ua3/"));
        List<Path> ua3ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "05/ua3/"));

        List<Path> ua2ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir2 + testDate + "00"), "_SUCCESS");
        List<Path> ua2ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir2 + testDate + "05"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua3ReplicatedData00, ua2ReplicatedData00);
        AssertUtil.checkForPathsSizes(ua3ReplicatedData05, ua2ReplicatedData05);
    }


    @Test(enabled = true)
    public void normalTest_1s1t1n_pt() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally
        // path for data in target cluster should also be customized
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), null, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}", testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE, null,
                testBaseDir1 + dateTemplate);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feed), "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feed), "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feed), "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                        .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feed), "RETENTION"), 1);


        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil.getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir2));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua3ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "00/ua2/"));
        List<Path> ua3ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "05/ua2/"));

        List<Path> ua2ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir2 + testDate + "00"), "_SUCCESS");
        List<Path> ua2ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir2 + testDate + "05"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua3ReplicatedData00, ua2ReplicatedData00);
        AssertUtil.checkForPathsSizes(ua3ReplicatedData05, ua2ReplicatedData05);
    }


    @Test(enabled = true)
    public void normalTest_1s2t_pt() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //cluster3 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on cluster1 and cluster2 as targets
        //ua3 is the source and ua1 and ua2 are target

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);
        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed, testBaseDir3 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}", null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}", null);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE, null, null);


        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(15000);

        InstanceUtil.waitTillInstanceReachState(cluster1OC, Util.getFeedName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.getFeedName(feed), 3,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1,
                        new Path(testBaseDir3 + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2", "ua3");

        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2,
                        new Path(testBaseDir3 + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua1ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir3 + testDate + "00/"), "_SUCCESS");
        List<Path> ua1ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir3 + testDate + "10/"), "_SUCCESS");

        List<Path> ua2ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir3 + testDate + "10"), "_SUCCESS");
        List<Path> ua2ReplicatedData15 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir3 + testDate + "15"), "_SUCCESS");

        List<Path> ua3OriginalData00ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir3 + testDate + "00/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir3 + testDate + "10/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir3 + testDate + "10/ua2"), "_SUCCESS");
        List<Path> ua3OriginalData15ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir3 + testDate + "15/ua2"), "_SUCCESS");

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

        // there are 2 source clusters cluster3 and cluster1
        //cluster2 is the target
        // Since there is no partition expression in source clusters, the feed submission should
        // fail (FALCON-305).

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, null,
                testBaseDir1 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET,
                "${cluster.colo}",
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE, null, null);

        //clean target if old data exists
        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        AssertUtil.assertFailed(r, "Submission of feed should have failed.");
        Assert.assertTrue(r.getMessage().contains(
                "Partition expression has to be specified for cluster " +
                        Util.readClusterName(bundles[0].getClusters().get(0)) +
                        " as there are more than one source clusters"),
                "Failed response has unexpected error message.");
    }


    @Test(enabled = true)
    public void normalTest_1s2t_ps() throws Exception {

        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //cluster3 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on cluster1 and cluster2 as targets
        //ua3 is the source and ua1 and ua2 are target
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed,
                testBaseDir1 + dateTemplate);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:11Z"),
                        XmlUtil.createRtention("days(10000000)", ActionType.DELETE),
                        Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET, null,
                        testBaseDir1 + "/ua1" + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:26Z"),
                        XmlUtil.createRtention("days(10000000)", ActionType.DELETE),
                        Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                        testBaseDir1 + "/ua2" + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", null);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(15000);

        InstanceUtil.waitTillInstanceReachState(cluster1OC, Util.getFeedName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir1 + "/ua1" + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2");

        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir1 + "/ua2" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1");


        List<Path> ua1ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir1 + "/ua1" + testDate + "00/"), "_SUCCESS");
        List<Path> ua1ReplicatedData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir1 + "/ua1" + testDate + "05/"), "_SUCCESS");

        List<Path> ua2ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir1 + "/ua2" + testDate + "10"), "_SUCCESS");
        List<Path> ua2ReplicatedData15 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir1 + "/ua2" + testDate + "15"), "_SUCCESS");

        List<Path> ua3OriginalData05ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "05/ua3"), "_SUCCESS");
        List<Path> ua3OriginalData10ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "10/ua3"), "_SUCCESS");
        List<Path> ua3OriginalData15ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "15/ua3"), "_SUCCESS");

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

        // there are 2 source clusters cluster3 and cluster1
        //cluster2 is the target
        //data should be replicated to cluster2 from ua2 sub dir of cluster3 and cluster1
        // source cluster path in cluster1 should be mentioned in cluster definition
        // path for data in target cluster should also be customized
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                        "${cluster.colo}",
                        testBaseDir1 + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                        testBaseDir2 + "/replicated" + dateTemplate);
        
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", testBaseDir1 + dateTemplate);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);
        Thread.sleep(15000);

        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.getFeedName(feed), 2,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil.getAllFilesRecursivelyHDFS(cluster2,
                new Path(testBaseDir2 + "/replicated" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua2");

        List<Path> ua2ReplicatedData00ua1 = HadoopUtil.getAllFilesRecursivelyHDFS(cluster2,
                new Path(testBaseDir2 + "/replicated" + testDate + "00/ua1"), "_SUCCESS");
        List<Path> ua2ReplicatedData05ua3 = HadoopUtil.getAllFilesRecursivelyHDFS(cluster2,
                new Path(testBaseDir2 + "/replicated" + testDate + "05/ua3/"), "_SUCCESS");


        List<Path> ua1OriginalData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir1 + testDate + "00/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData05 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster3, new Path(testDirWithDate + "05/ua1"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua2ReplicatedData00ua1, ua1OriginalData00);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData05ua3, ua3OriginalData05);
    }


    @Test(enabled = true)
    public void normalTest_1s2t_pst() throws Exception {


        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //cluster3 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on cluster1 and cluster2 as targets
        //ua3 is the source and ua1 and ua2 are target
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed, testBaseDir1 + dateTemplate);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
                        "${cluster.colo}", testBaseDir1 + "/ua1" + dateTemplate + "/");
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                        XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                        Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET,
                        "${cluster.colo}", testBaseDir1 + "/ua2" + dateTemplate + "/");
        
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                , XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", null);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(15000);        
        InstanceUtil.waitTillInstanceReachState(cluster1OC, Util.getFeedName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.getFeedName(feed), 3,
                CoordinatorAction.Status.SUCCEEDED, 7, ENTITY_TYPE.FEED);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir1 + "/ua1" + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2", "ua3");

        List<Path> ua2ReplicatedData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir1 + "/ua2" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua1ReplicatedData00 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir1 + "/ua1" + testDate + "00/"), "_SUCCESS");
        List<Path> ua1ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster1, new Path(testBaseDir1 + "/ua1" + testDate + "10/"), "_SUCCESS");

        List<Path> ua2ReplicatedData10 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir1 + "/ua2" + testDate + "10"), "_SUCCESS");
        List<Path> ua2ReplicatedData15 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testBaseDir1 + "/ua2" + testDate + "15"), "_SUCCESS");

        List<Path> ua3OriginalData00ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testDirWithDate + "00/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua1 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testDirWithDate + "10/ua1"), "_SUCCESS");
        List<Path> ua3OriginalData10ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testDirWithDate + "10/ua2"), "_SUCCESS");
        List<Path> ua3OriginalData15ua2 = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testDirWithDate + "15/ua2"), "_SUCCESS");

        AssertUtil.checkForPathsSizes(ua1ReplicatedData00, new ArrayList<Path>());
        AssertUtil.checkForPathsSizes(ua1ReplicatedData10, ua3OriginalData10ua1);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForPathsSizes(ua2ReplicatedData15, ua3OriginalData15ua2);
    }


    @Test(enabled = true)
    public void moreThanOneClusterWithSameNameDiffValidity() throws Exception {
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2012-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, "",
                testBaseDir1 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2012-10-01T12:25Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.TARGET, "",
                testBaseDir2 + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE, "", null);

        Util.print("feed: " + feed);

        ServiceResponse r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertFailed(r, "is defined more than once for feed");
        Assert.assertTrue(r.getMessage().contains("is defined more than once for feed"));
    }
}
