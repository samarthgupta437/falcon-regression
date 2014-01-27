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
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

public class PrismFeedLateReplicationTest extends BaseMultiClusterTests {

    private String normalInputPath = "src/test/resources/OozieExampleInputData/normalInput";
    private Bundle bundle1, bundle2, bundle3;
    
    private String server1Colo = server1.getClusterHelper().getColo().split("=")[1];
    private String server2Colo = server2.getClusterHelper().getColo().split("=")[1];
    private String server3Colo = server3.getClusterHelper().getColo().split("=")[1];
    
    private String inputPath = baseHDFSDir + "/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = Util.readELBundles()[0][0];

        bundle1 = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());
        bundle2 = new Bundle(bundle, server2.getEnvFileName(), server2.getPrefix());
        bundle3 = new Bundle(bundle, server3.getEnvFileName(), server3.getPrefix());

        bundle1.generateUniqueBundle();
        bundle2.generateUniqueBundle();
        bundle3.generateUniqueBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        bundle1.deleteBundle(prism);
        bundle2.deleteBundle(prism);
        bundle3.deleteBundle(prism);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = {"multiCluster"})
    public void multipleSourceOneTarget_pastData() throws Exception {

        bundle1.setInputFeedDataPath(inputPath);

        bundle1.setCLusterColo(server1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        Util.assertSucceeded(r);

        bundle2.setCLusterColo(server2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Util.assertSucceeded(r);

        bundle3.setCLusterColo(server3Colo);
        Util.print("cluster bundle3: " + bundle3.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle3.getClusters().get(0));
        Util.assertSucceeded(r);

        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);

        String postFix = "/US/ua2";
        String prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server2FS);
        Util.lateDataReplenish(server2, 90, 1, prefix, postFix);


        postFix = "/UK/ua3";
        prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server3FS);
        Util.lateDataReplenish(server3, 90, 1, prefix, postFix);

        String startTime = InstanceUtil.getTimeWrtSystemTime(-30);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}");

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
                        null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                        "UK/${cluster.colo}");


        Util.print("feed: " + feed);

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Thread.sleep(10000);

        String bundleId = InstanceUtil.getLatestBundleID(server1, Util.readDatasetName(feed), ENTITY_TYPE.FEED);

        //wait till 1st instance of replication coord is SUCCEEDED
        List<String> replicationCoordIDTarget = InstanceUtil
                .getReplicationCoordID(bundleId, server1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(0), 0)
                    == WorkflowJob.Status.SUCCEEDED
                    && InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(1), 0)
                    == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            Thread.sleep(20000);
        }

        Thread.sleep(15000);

        List<String> inputFolderListForColo1 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(0),
                        0);
        List<String> inputFolderListForColo2 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(1),
                        0);

        Util.print("folder list 1: " + inputFolderListForColo1.toString());
        Util.print("folder list 2: " + inputFolderListForColo2.toString());

        HadoopUtil.flattenAndPutDataInFolder(server2FS, normalInputPath, inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(server3FS, normalInputPath, inputFolderListForColo2);

        Util.print("test");
    }

    @Test(groups = {"multiCluster"})
    public void multipleSourceOneTarget_futureData() throws Exception {

        bundle1.setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundle1, bundle2, bundle3);

        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);


        String startTime = InstanceUtil.getTimeWrtSystemTime(3);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}", null);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
                        null, null);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                        "UK/${cluster.colo}", null);


        Util.print("feed: " + feed);

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Thread.sleep(10000);

        String postFix = "/US/ua2";
        String prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server2FS);
        Util.lateDataReplenish(server2, 90, 1, prefix, postFix);

        postFix = "/UK/ua3";
        prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server3FS);
        Util.lateDataReplenish(server3, 90, 1, prefix, postFix);

        Thread.sleep(60000);

        //wait till 1st instance of replication coord is SUCCEEDED
        String bundleId = InstanceUtil
                .getLatestBundleID(server1, Util.readDatasetName(feed), ENTITY_TYPE.FEED);

        List<String> replicationCoordIDTarget = InstanceUtil.getReplicationCoordID(bundleId, server1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(0), 0)
                    == WorkflowJob.Status.SUCCEEDED
                    && InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(1), 0)
                    == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            Util.print("still in for loop");
            Thread.sleep(20000);
        }

        Thread.sleep(15000);

        List<String> inputFolderListForColo1 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(0), 1);
        List<String> inputFolderListForColo2 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(1), 1);

        Util.print("folder list 1: " + inputFolderListForColo1.toString());
        Util.print("folder list 2: " + inputFolderListForColo2.toString());

        HadoopUtil.flattenAndPutDataInFolder(server2FS, normalInputPath, inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(server3FS, normalInputPath, inputFolderListForColo2);

        //sleep till late starts
        InstanceUtil.sleepTill(server1, InstanceUtil.addMinsToTime(startTime, 4));

        //check for run id to  be 1
        Assert.assertTrue(InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(0), 0) == 1
                && InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(1), 0) == 1 , 
                "id have to be equal 1");


        //wait for lates run to complete
        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(0), 0)
                    == WorkflowJob.Status.SUCCEEDED
                    && InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(1), 0)
                    == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            Util.print("still in for loop");
            Thread.sleep(20000);
        }


        Thread.sleep(30000);

        //put data for the second time
        InstanceUtil.putLateDataInFolders(server2, inputFolderListForColo1, 2);
        InstanceUtil.putLateDataInFolders(server3, inputFolderListForColo2, 2);

        //sleep till late 2 starts
        InstanceUtil.sleepTill(server1, InstanceUtil.addMinsToTime(startTime, 9));

        //check for run id to be 2
        Assert.assertTrue(InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(0), 0) == 2
                && InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(1), 0) == 2, 
                "id have to be equal 2");
    }

    /** this test case does the following
     *  two source ua2 and ua3
     *  ua3 has following part data
     *  ua1/ua2
     *  ua1/ua2
     *  ua1/ua2
     *
     *  ua2 has following part data
     *  ua1/ua3
     *  ua1/ua3
     *  ua1/ua3
     *
     *  ua1 is the target, which in the end should have all ua1 data
     *
     *  after first instance succeed data in put into relevant source and late should rerun
     *
     *  after first late succeed data is put into other source and late should not  */

    @Test(groups = {"multiCluster"})
    public void mixedTest01() throws Exception {

        bundle1.setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundle1, bundle2, bundle3);


        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);


        String startTime = InstanceUtil.getTimeWrtSystemTime(3);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                "ua1/${cluster.colo}", null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
                null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                "ua1/${cluster.colo}", null);

        Util.print("feed: " + feed);
        Thread.sleep(15000);

        //submit and schedule feed
        Util.print("feed: " + feed);

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Thread.sleep(10000);

        //wait till 1st instance of replication coord is SUCCEEDED
        String bundleId = InstanceUtil.getLatestBundleID(server1, Util.readDatasetName(feed), ENTITY_TYPE.FEED);

        List<String> replicationCoordIDTarget = InstanceUtil.getReplicationCoordID(bundleId, server1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(0), 0)
                    == WorkflowJob.Status.SUCCEEDED
                    && InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(1), 0)
                    == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            Util.print("still in for loop");
            Thread.sleep(20000);
        }

        Thread.sleep(15000);

        //check for exact folders to be created in ua1 :  ua1/ua2 and ua1/ua3 no other should
        // be present. both of them should have _success


        List<String> inputFolderListForColo1 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(0), 1);
        List<String> inputFolderListForColo2 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(1), 1);

        String outPutLocation = InstanceUtil
                .getOutputFolderForInstanceForReplication(server1, replicationCoordIDTarget.get(0), 1);
        String outPutBaseLocation = InstanceUtil
                .getOutputFolderBaseForInstanceForReplication(server1, replicationCoordIDTarget.get(0), 1);

        List<String> subfolders = HadoopUtil.getHDFSSubFoldersName(server1FS, outPutBaseLocation);

        Assert.assertTrue(subfolders.size() == 1 && subfolders.get(0).equals("ua1"));

        Assert.assertFalse(HadoopUtil.isFilePresentHDFS(server1, outPutBaseLocation, "_SUCCESS"));

        Assert.assertTrue(HadoopUtil.isFilePresentHDFS(server1, outPutLocation, "_SUCCESS"));

        Util.print("folder list 1: " + inputFolderListForColo1.toString());
        Util.print("folder list 2: " + inputFolderListForColo2.toString());

        HadoopUtil.flattenAndPutDataInFolder(server2FS, normalInputPath, inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(server3FS, normalInputPath, inputFolderListForColo2);

        //sleep till late starts
        InstanceUtil.sleepTill(server1, InstanceUtil.addMinsToTime(startTime, 4));

        //check for run id to  be 1
        Assert.assertTrue(InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(0), 0) == 1
                && InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(1), 0) == 1, 
                "id have to be equal 1");


        //wait for lates run to complete
        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(0), 0)
                    == WorkflowJob.Status.SUCCEEDED
                    && InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(1), 0)
                    == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            Util.print("still in for loop");
            Thread.sleep(20000);
        }


        Thread.sleep(30000);

        //put data for the second time
        InstanceUtil.putLateDataInFolders(server2, inputFolderListForColo1, 2);
        InstanceUtil.putLateDataInFolders(server3, inputFolderListForColo2, 2);

        //sleep till late 2 starts
        InstanceUtil.sleepTill(server1, InstanceUtil.addMinsToTime(startTime, 9));

        //check for run id to be 2
        Assert.assertTrue(InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(0), 0) == 2
                && InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(1), 0) == 2, 
                "id have to be equal 2");
    }

    /**     only difference between mixed 01 and 02 is of availability flag. feed has _success as
     availability flag ...so replication should not start till _success is put in ua2

     this test case does the following
     two source ua2 and ua3
     ua3 has follwing part data
     ua1/ua2
     ua1/ua2
     ua1/ua2

     ua2 has following part data
     ua1/ua3
     ua1/ua3
     ua1/ua3

     ua1 is the target, which in the end should have all ua1 data
     after first instance succeed data in put into relevant source and late should rerun
     after first late succeed data is put into other source and late should not rerun */
    @Test(groups = {"multiCluster"})
    public void mixedTest02() throws Exception {
        bundle1.setInputFeedDataPath(inputPath);

        bundle1.setCLusterColo(server1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        Util.assertSucceeded(r);

        bundle2.setCLusterColo(server2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Util.assertSucceeded(r);


        bundle3.setCLusterColo(server3Colo);
        Util.print("cluster bundle3: " + bundle3.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle3.getClusters().get(0));
        Util.assertSucceeded(r);

        //set availability flag as _success
        bundle1.setInputFeedAvailabilityFlag("_SUCCESS");

        //get feed
        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        String startTime = InstanceUtil.getTimeWrtSystemTime(3);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                "ua1/${cluster.colo}", null);
        
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
                null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                "ua1/${cluster.colo}", null);

        //create data in colos

        String postFix = "/ua1/ua2";
        String prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server2FS);
        Util.lateDataReplenishWithout_Success(server2, 90, 1, prefix, postFix);

        postFix = "/ua2/ua2";
        Util.lateDataReplenishWithout_Success(server2, 90, 1, prefix, postFix);

        postFix = "/ua3/ua2";
        Util.lateDataReplenishWithout_Success(server2, 90, 1, prefix, postFix);

        //put _SUCCESS in parent folder UA2
        Util.putFileInFolderHDFS(server2, 90, 1, prefix, "_SUCCESS");

        postFix = "/ua1/ua3";
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server3FS);
        Util.lateDataReplenish(server3, 90, 1, prefix, postFix);

        postFix = "/ua2/ua3";
        Util.lateDataReplenish(server3, 90, 1, prefix, postFix);

        postFix = "/ua3/ua3";
        Util.lateDataReplenish(server3, 90, 1, prefix, postFix);

        //put _SUCCESS in parent folder of UA3
        Util.putFileInFolderHDFS(server3, 90, 1, prefix, "_SUCCESS");

        Thread.sleep(15000);

        //submit and schedule feed
        Util.print("feed: " + feed);

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Thread.sleep(10000);

        //wait till 1st instance of replication coord is SUCCEEDED
        String bundleId = InstanceUtil.getLatestBundleID(server1, Util.readDatasetName(feed), ENTITY_TYPE.FEED);

        List<String> replicationCoordIDTarget = InstanceUtil.getReplicationCoordID(bundleId, server1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(0), 0)
                    == WorkflowJob.Status.SUCCEEDED
                    && InstanceUtil.getInstanceStatusFromCoord(server1, replicationCoordIDTarget.get(1), 0)
                    == WorkflowJob.Status.SUCCEEDED) {
                break;
            }

            Util.print("still in for loop");
            Thread.sleep(20000);
        }

        Thread.sleep(15000);

        /* check for exact folders to be created in ua1 :  ua1/ua2 and ua1/ua3 no other should
           be present. both of
           them should have _success */
        List<String> inputFolderListForColo1 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(0), 0);
        List<String> inputFolderListForColo2 = InstanceUtil
                .getInputFoldersForInstanceForReplication(server1, replicationCoordIDTarget.get(1), 0);

        String outPutLocation = InstanceUtil
                .getOutputFolderForInstanceForReplication(server1, replicationCoordIDTarget.get(0), 0);
        String outPutBaseLocation = InstanceUtil
                .getOutputFolderBaseForInstanceForReplication(server1, replicationCoordIDTarget.get(0), 0);

        List<String> subfolders = HadoopUtil.getHDFSSubFoldersName(server1FS, outPutBaseLocation);

        Assert.assertTrue(subfolders.size() == 1 && subfolders.get(0).equals("ua1"));

        Assert.assertFalse(HadoopUtil.isFilePresentHDFS(server1, outPutBaseLocation, "_SUCCESS"));

        Assert.assertTrue(HadoopUtil.isFilePresentHDFS(server1, outPutLocation, "_SUCCESS"));

        Util.print("folder list 1: " + inputFolderListForColo1.toString());
        Util.print("folder list 2: " + inputFolderListForColo2.toString());

        HadoopUtil.flattenAndPutDataInFolder(server2FS, normalInputPath, inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(server3FS, normalInputPath, inputFolderListForColo2);

        //sleep till late starts
        InstanceUtil.sleepTill(server1, InstanceUtil.addMinsToTime(startTime, 4));

        //check for run id to  be 1
        Assert.assertTrue(InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(0), 0) == 1
                && InstanceUtil.getInstanceRunIdFromCoord(server1, replicationCoordIDTarget.get(1), 0) == 1, 
                "id have to be equal 1");
    }
}
