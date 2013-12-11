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

package org.apache.falcon.regression;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Process instance mixed colo tests.
 */
@SuppressWarnings("deprecation")
public class ProcessInstanceColoMixedTest {

    private final PrismHelper prismHelper = new PrismHelper("prism.properties");
    private final ColoHelper ua1 = new ColoHelper("mk-qa.config.properties");
    private final ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");
    private final ColoHelper ua3 = new ColoHelper("gs1001.config.properties");


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Util.restartService(prismHelper.getClusterHelper());
        Util.restartService(ua1.getClusterHelper());
        Util.restartService(ua3.getClusterHelper());
    }

    @Test(timeOut = 12000000)
    public void mixed01_C1sC2sC1eC2e() throws Exception {
        //ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3

        //get 3 unique bundles
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            //generate bundles according to config files
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());

            //set cluster colos
            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));
            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            b3.setCLusterColo("ua3");
            Util.print("cluster b3: " + b3.getClusters().get(0));


            //submit 3 clusters
            Bundle.submitCluster(b1, b2, b3);

            //get 2 unique feeds
            String feed01 = Util.getInputFeedFromBundle(b1);
            String feed02 = Util.getInputFeedFromBundle(b2);
            String outputFeed = Util.getOutputFeedFromBundle(b1);

            //set source and target for the 2 feeds

            //set clusters to null;
            feed01 = InstanceUtil
                    .setFeedCluster(feed01,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);
            feed02 = InstanceUtil
                    .setFeedCluster(feed02,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);
            outputFeed = InstanceUtil
                    .setFeedCluster(outputFeed,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);


            //set new feed input data
            feed01 = Util.setFeedPathValue(feed01,
                    "/samarthRetention/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
            feed02 = Util.setFeedPathValue(feed02,
                    "/samarthRetention/feed02/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


            //generate data in both the colos ua1 and ua3
            String prefix = InstanceUtil.getFeedPrefix(feed01);
            Util.HDFSCleanup(ua1, prefix.substring(1));
            //Util.lateDataReplenish(ua1,80,0,1,prefix);
            InstanceUtil.createDataWithinDatesAndPrefix(ua1,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-100)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(100)), prefix,
                    1);


            prefix = InstanceUtil.getFeedPrefix(feed02);
            Util.HDFSCleanup(ua3, prefix.substring(1));
            //	Util.lateDataReplenish(ua3,80,0,1,prefix);
            InstanceUtil.createDataWithinDatesAndPrefix(ua3,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-100)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(100)), prefix,
                    1);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-70);

            //set clusters for feed01
            feed01 = InstanceUtil
                    .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed01 = InstanceUtil
                    .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET,
                            null);


            //set clusters for feed02
            feed02 = InstanceUtil
                    .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET,
                            null);
            feed02 = InstanceUtil
                    .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                            null);

            //set clusters for output feed
            outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                    XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                    XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE, null);
            outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                    XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                    XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET, null);


            //submit and schedule feeds
            Util.print("feed01: " + feed01);
            Util.print("feed02: " + feed02);
            Util.print("outputFeed: " + outputFeed);

            ServiceResponse r = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed01);
            AssertUtil.assertSucceeded(r);
            r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed02);
            AssertUtil.assertSucceeded(r);
            r = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, outputFeed);
            AssertUtil.assertSucceeded(r);


            //create a process with 2 clusters

            //get a process
            String process = b1.getProcessData();

            //add clusters to process

            String processStartTime = InstanceUtil.getTimeWrtSystemTime(-16);
            // String processEndTime = InstanceUtil.getTimeWrtSystemTime(20);

            process = InstanceUtil
                    .setProcessCluster(process, null,
                            XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
            process = InstanceUtil
                    .setProcessCluster(process, Util.readClusterName(b1.getClusters().get(0)),
                            XmlUtil.createProcessValidity(processStartTime,
                                    InstanceUtil.addMinsToTime(processStartTime, 35)));
            process = InstanceUtil
                    .setProcessCluster(process, Util.readClusterName(b3.getClusters().get(0)),
                            XmlUtil.createProcessValidity(
                                    InstanceUtil.addMinsToTime(processStartTime, 16),
                                    InstanceUtil.addMinsToTime(processStartTime, 45)));
            process = InstanceUtil
                    .addProcessInputFeed(process, Util.readDatasetName(feed02),
                            Util.readDatasetName(feed02));


            //submit and schedule process
            Util.print("process: " + process);

            prismHelper.getProcessHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);

            Util.print("Wait till process goes into running ");

            int i;

            for (i = 0; i < 30; i++) {
                Status sUa1 =
                        InstanceUtil.getInstanceStatus(ua1, Util.getProcessName(process), 0, 0);
                Status sUa2 =
                        InstanceUtil.getInstanceStatus(ua3, Util.getProcessName(process), 0, 0);
                if ((sUa1.toString().equals("RUNNING") || (sUa1.toString().equals("SUCCEEDED"))) &&
                        (sUa2.toString().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED")))
                    break;
                Thread.sleep(20000);

            }

            if (i == 30)
                Assert.assertTrue(false);

            ProcessInstancesResult responseInstance = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b1.getProcessData()),
                            "?start=" + processStartTime + "&end=" + InstanceUtil
                                    .addMinsToTime(processStartTime, 45));
            Util.assertSucceeded(responseInstance);
            Assert.assertTrue(responseInstance.getInstances() != null);

            responseInstance = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b1.getProcessData()),
                            "?start=" + InstanceUtil
                                    .addMinsToTime(processStartTime, 37) + "&end=" +
                                    InstanceUtil.addMinsToTime(processStartTime, 44));
            Util.assertSucceeded(responseInstance);
            Assert.assertTrue(responseInstance.getInstances() != null);

            responseInstance = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b1.getProcessData()),
                            "?start=" + InstanceUtil
                                    .addMinsToTime(processStartTime, 37) + "&end=" +
                                    InstanceUtil.addMinsToTime(processStartTime, 44));
            Util.assertSucceeded(responseInstance);
            Assert.assertTrue(responseInstance.getInstances() == null);

            responseInstance = prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b1.getProcessData()),
                            "?start=" + processStartTime + "&end=" + InstanceUtil
                                    .addMinsToTime(processStartTime, 7));
            Util.assertSucceeded(responseInstance);
            Assert.assertTrue(responseInstance.getInstances() != null);

            responseInstance = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b1.getProcessData()),
                            "?start=" + InstanceUtil
                                    .addMinsToTime(processStartTime, 16) + "&end=" +
                                    InstanceUtil.addMinsToTime(processStartTime, 45));
            Util.assertSucceeded(responseInstance);
            Assert.assertTrue(responseInstance.getInstances() != null);

            responseInstance = ua1.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b1.getProcessData()),
                            "?start=" + processStartTime + "&end=" + InstanceUtil
                                    .addMinsToTime(processStartTime, 7));
            Util.assertSucceeded(responseInstance);
            Assert.assertTrue(responseInstance.getInstances() != null);

            responseInstance = prismHelper.getProcessHelper()
                    .getProcessInstanceRerun(Util.readEntityName(b1.getProcessData()),
                            "?start=" + processStartTime + "&end=" + InstanceUtil
                                    .addMinsToTime(processStartTime, 7));
            Util.assertSucceeded(responseInstance);
            Assert.assertTrue(responseInstance.getInstances() != null);


        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);

        }
    }


	
/*	@Test(timeOut=12000000)
    public void mixed01_SsSeTsTe() throws Exception
	{
		//ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3
		
		//get 3 unique bundles
		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Util.readELBundles()[0][0];
		b3.generateUniqueBundle();

		try
		{
			//generate bundles according to config files
			b1  = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());

			//set cluster colos
			b1.setCLusterColo("ua1");
			Util.print("cluster b1: "+b1.getClusters().get(0));
			b2.setCLusterColo("ua2");
			Util.print("cluster b2: "+b2.getClusters().get(0));
			b3.setCLusterColo("ua3");
			Util.print("cluster b3: "+b3.getClusters().get(0));


			//submit 3 clusters
			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b1.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b2.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b3.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			//get 2 unique feeds
			String feed01 = Util.getInputFeedFromBundle(b1);
			String feed02 = Util.getInputFeedFromBundle(b2);
			String outputFeed = Util.getOutputFeedFromBundle(b1);

			//set source and target for the 2 feeds

			//set clusters to null;
			feed01 =  instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity
			("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);
			feed02 =  instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity
			("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);
			outputFeed =  instanceUtil.setFeedCluster(outputFeed,
			XmlUtil.createValidity("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);


			//set new feed input data
			feed01 = 	Util.setFeedPathValue(feed01,
			"/samarthRetention/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed02 = 	Util.setFeedPathValue(feed02,
			"/samarthRetention/feed02/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


			//generate data in both the colos ua1 and ua3
			String prefix = instanceUtil.getFeedPrefix(feed01);
			Util.HDFSCleanup(ua1,prefix.substring(1));
			Util.lateDataReplenish(ua1,80,0,1,prefix);


			prefix = instanceUtil.getFeedPrefix(feed02);
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.lateDataReplenish(ua3,80,0,1,prefix);


			String startTime = instanceUtil.getTimeWrtSystemTime(-70);

			//set clusters for feed01 
			feed01 = instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.SOURCE,null);
			feed01 = instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.TARGET,null);


			//set clusters for feed02
			feed02 = instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.TARGET,null);
			feed02 = instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.SOURCE,null);

			//set clusters for output feed
			outputFeed = instanceUtil.setFeedCluster(outputFeed,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.SOURCE,null);
			outputFeed = instanceUtil.setFeedCluster(outputFeed,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.TARGET,null);



			//submit and schedule feeds
			Util.print("feed01: "+feed01);
			Util.print("feed02: "+feed02);
			Util.print("outputFeed: "+outputFeed);

			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			feed01);
			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			feed02);
			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, outputFeed);


			//create a process with 2 clusters 

			//get a process
			String process = b1.getProcessData();

			//add clusters to process

			String 	processStartTime = instanceUtil.getTimeWrtSystemTime(-16);
			String 	processEndTime = instanceUtil.getTimeWrtSystemTime(20);


			process = instanceUtil.setProcessCluster(process,null,XmlUtil.createProcessValidity
			(startTime,
			"2099-01-01T00:00Z"));
			process = instanceUtil.setProcessCluster(process,Util.readClusterName(b1.getClusters()
			.get(0)),
			XmlUtil.createProcessValidity(processStartTime,instanceUtil.addMinsToTime
			(processStartTime, 10)));
			process = instanceUtil.setProcessCluster(process,Util.readClusterName(b3.getClusters()
			.get(0)),
			XmlUtil.createProcessValidity(instanceUtil.addMinsToTime(processStartTime, 16),
			instanceUtil.addMinsToTime(processStartTime,45)));
			process = instanceUtil.addProcessInputFeed(process,Util.readDatasetName(feed02),
			Util.readDatasetName(feed02));


			//submit and schedule process
			Util.print("process: "+process);

			r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			process);
			
			ProcessInstancesResult responseInstance  =  prismHelper.getProcessHelper()
			.getProcessInstanceStatus(Util
			.readEntityName(b1.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil
			.addMinsToTime
			(processStartTime,45));


			Util.print("Wait till process goes into running ");

			int i =0 ;

			for(i = 0 ; i < 30 ; i++)
			{
				Status sUa1 = 	instanceUtil.getInstanceStatus(ua1, Util.getProcessName(process),
				0, 0);
				Status sUa2 =	instanceUtil.getInstanceStatus(ua3, Util.getProcessName(process),
				0, 0);
				Log.info("us1 status: "+sUa1.toString() + " ua3 status: "+sUa2.toString());
				if((sUa1.toString().equals("RUNNING") || (sUa1.toString().equals("SUCCEEDED")))
				&&  (sUa2.toString()
				.equals("RUNNING") || sUa2.toString().equals("SUCCEEDED")) )
					break;
				Thread.sleep(20000);
				
			}
			
			if(i == 30)
				Assert.assertTrue(false);
			
			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime,45));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);
			
			responseInstance  =  ua1.getProcessHelper().getProcessInstanceSuspend(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime, 35));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util
			.readEntityName(b1
			.getProcessData()),"?start="+instanceUtil.addMinsToTime(processStartTime,
			11)+"&end="+instanceUtil.addMinsToTime(processStartTime, 15));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()==null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceResume(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime, 7));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util
			.readEntityName(b1
			.getProcessData()),"?start="+instanceUtil.addMinsToTime(processStartTime,
			16)+"&end="+instanceUtil.addMinsToTime(processStartTime,45));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  ua1.getProcessHelper().getProcessInstanceKill(Util.readEntityName
			(b1.getProcessData()
			),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime(processStartTime, 7));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceRerun(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime, 7));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);





		}
		finally
		{
			b1.deleteBundle(prismHelper);
			b2.deleteBundle(prismHelper);
			b3.deleteBundle(prismHelper);

		}
	}
	
	
	
	@Test(timeOut=12000000)
	public void mixed01_C1sC2sC2eC1e() throws Exception
	{
		//ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3
		
		//get 3 unique bundles
		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Util.readELBundles()[0][0];
		b3.generateUniqueBundle();

		try
		{
			//generate bundles according to config files
			b1  = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());

			//set cluster colos
			b1.setCLusterColo("ua1");
			Util.print("cluster b1: "+b1.getClusters().get(0));
			b2.setCLusterColo("ua2");
			Util.print("cluster b2: "+b2.getClusters().get(0));
			b3.setCLusterColo("ua3");
			Util.print("cluster b3: "+b3.getClusters().get(0));


			//submit 3 clusters
			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b1.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b2.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b3.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			//get 2 unique feeds
			String feed01 = Util.getInputFeedFromBundle(b1);
			String feed02 = Util.getInputFeedFromBundle(b2);
			String outputFeed = Util.getOutputFeedFromBundle(b1);

			//set source and target for the 2 feeds

			//set clusters to null;
			feed01 =  instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity
			("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);
			feed02 =  instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity
			("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);
			outputFeed =  instanceUtil.setFeedCluster(outputFeed,
			XmlUtil.createValidity("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);


			//set new feed input data
			feed01 = 	Util.setFeedPathValue(feed01,
			"/samarthRetention/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed02 = 	Util.setFeedPathValue(feed02,
			"/samarthRetention/feed02/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


			//generate data in both the colos ua1 and ua3
			String prefix = instanceUtil.getFeedPrefix(feed01);
			Util.HDFSCleanup(ua1,prefix.substring(1));
			Util.lateDataReplenish(ua1,80,0,1,prefix);


			prefix = instanceUtil.getFeedPrefix(feed02);
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.lateDataReplenish(ua3,80,0,1,prefix);


			String startTime = instanceUtil.getTimeWrtSystemTime(-70);

			//set clusters for feed01 
			feed01 = instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.SOURCE,null);
			feed01 = instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.TARGET,null);


			//set clusters for feed02
			feed02 = instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.TARGET,null);
			feed02 = instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.SOURCE,null);

			//set clusters for output feed
			outputFeed = instanceUtil.setFeedCluster(outputFeed,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.SOURCE,null);
			outputFeed = instanceUtil.setFeedCluster(outputFeed,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.TARGET,null);



			//submit and schedule feeds
			Util.print("feed01: "+feed01);
			Util.print("feed02: "+feed02);
			Util.print("outputFeed: "+outputFeed);

			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			feed01);
			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			feed02);
			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, outputFeed);


			//create a process with 2 clusters 

			//get a process
			String process = b1.getProcessData();

			//add clusters to process

			String 	processStartTime = instanceUtil.getTimeWrtSystemTime(-16);
			String 	processEndTime = instanceUtil.getTimeWrtSystemTime(20);


			process = instanceUtil.setProcessCluster(process,null,XmlUtil.createProcessValidity
			(startTime,
			"2099-01-01T00:00Z"));
			process = instanceUtil.setProcessCluster(process,Util.readClusterName(b1.getClusters()
			.get(0)),
			XmlUtil.createProcessValidity(processStartTime,instanceUtil.addMinsToTime
			(processStartTime, 45)));
			process = instanceUtil.setProcessCluster(process,Util.readClusterName(b3.getClusters()
			.get(0)),
			XmlUtil.createProcessValidity(instanceUtil.addMinsToTime(processStartTime, 16),
			instanceUtil.addMinsToTime(processStartTime,37)));
			process = instanceUtil.addProcessInputFeed(process,Util.readDatasetName(feed02),
			Util.readDatasetName(feed02));


			//submit and schedule process
			Util.print("process: "+process);

			r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			process);

			Util.print("Wait till process goes into running ");

			int i =0 ;

			for(i = 0 ; i < 30 ; i++)
			{
				Status sUa1 = 	instanceUtil.getInstanceStatus(ua1, Util.getProcessName(process),
				0, 0);
				Status sUa2 =	instanceUtil.getInstanceStatus(ua3, Util.getProcessName(process),
				0, 0);
				if((sUa1.toString().equals("RUNNING") || (sUa1.toString().equals("SUCCEEDED ")))
				 &&  (sUa2.toString
				().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED")) )
					break;
				Thread.sleep(20000);
				
			}
			
			if(i == 30)
				Assert.assertTrue(false);
			
			ProcessInstancesResult responseInstance  =  prismHelper.getProcessHelper()
			.getProcessInstanceStatus(Util
			.readEntityName(b1.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil
			.addMinsToTime
			(processStartTime,45));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);
			
			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceSuspend(Util
			.readEntityName(b1
			.getProcessData()),"?start="+instanceUtil.addMinsToTime(processStartTime,
			16)+"&end="+instanceUtil.addMinsToTime(processStartTime,37));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util
			.readEntityName(b1
			.getProcessData()),"?start="+instanceUtil.addMinsToTime(processStartTime,
			37)+"&end="+instanceUtil.addMinsToTime(processStartTime, 44));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()==null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceResume(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime, 7));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util
			.readEntityName(b1
			.getProcessData()),"?start="+instanceUtil.addMinsToTime(processStartTime,
			16)+"&end="+instanceUtil.addMinsToTime(processStartTime,45));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  ua1.getProcessHelper().getProcessInstanceKill(Util.readEntityName
			(b1.getProcessData()
			),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime(processStartTime, 7));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceRerun(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime, 7));
			Util.assertSucceeded(responseInstance);
			Assert.assertTrue(responseInstance.getInstances()!=null);





		}
		finally
		{
			b1.deleteBundle(prismHelper);
			b2.deleteBundle(prismHelper);
			b3.deleteBundle(prismHelper);

		}
	}
	
	@Test(timeOut=12000000)
	public void mixed01_allValiditySame() throws Exception
	{
		//ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3
		
		//get 3 unique bundles
		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Util.readELBundles()[0][0];
		b3.generateUniqueBundle();

		try
		{
			//generate bundles according to config files
			b1  = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());

			//set cluster colos
			b1.setCLusterColo("ua1");
			Util.print("cluster b1: "+b1.getClusters().get(0));
			b2.setCLusterColo("ua2");
			Util.print("cluster b2: "+b2.getClusters().get(0));
			b3.setCLusterColo("ua3");
			Util.print("cluster b3: "+b3.getClusters().get(0));


			//submit 3 clusters
			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b1.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b2.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
			b3.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			//get 2 unique feeds
			String feed01 = Util.getInputFeedFromBundle(b1);
			String feed02 = Util.getInputFeedFromBundle(b2);
			String outputFeed = Util.getOutputFeedFromBundle(b1);

			//set source and target for the 2 feeds

			//set clusters to null;
			feed01 =  instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity
			("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);
			feed02 =  instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity
			("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);
			outputFeed =  instanceUtil.setFeedCluster(outputFeed,
			XmlUtil.createValidity("2009-02-01T00:00Z",
			"2012-01-01T00:00Z"),XmlUtil.createRtention("hours(10)",ActionType.DELETE),null,
			ClusterType.SOURCE,null);


			//set new feed input data
			feed01 = 	Util.setFeedPathValue(feed01,
			"/samarthRetention/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed02 = 	Util.setFeedPathValue(feed02,
			"/samarthRetention/feed02/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


			//generate data in both the colos ua1 and ua3
			String prefix = instanceUtil.getFeedPrefix(feed01);
			Util.HDFSCleanup(ua1,prefix.substring(1));
			Util.lateDataReplenish(ua1,90,0,1,prefix);


			prefix = instanceUtil.getFeedPrefix(feed02);
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.lateDataReplenish(ua3,90,0,1,prefix);


			String startTime = instanceUtil.getTimeWrtSystemTime(-50);

			//set clusters for feed01 
			feed01 = instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.SOURCE,null);
			feed01 = instanceUtil.setFeedCluster(feed01,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.TARGET,null);


			//set clusters for feed02
			feed02 = instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.TARGET,null);
			feed02 = instanceUtil.setFeedCluster(feed02,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.SOURCE,null);

			//set clusters for output feed
			outputFeed = instanceUtil.setFeedCluster(outputFeed,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1
			.getClusters().get(0)),
			ClusterType.SOURCE,null);
			outputFeed = instanceUtil.setFeedCluster(outputFeed,XmlUtil.createValidity(startTime,
			"2099-01-01T00:00Z"),
			XmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3
			.getClusters().get(0)),
			ClusterType.TARGET,null);



			//submit and schedule feeds
			Util.print("feed01: "+feed01);
			Util.print("feed02: "+feed02);
			Util.print("outputFeed: "+outputFeed);

			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			feed01);
			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			feed02);
			r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, outputFeed);


			//create a process with 2 clusters 

			//get a process
			String process = b1.getProcessData();

			//add clusters to process

			String 	processStartTime = instanceUtil.getTimeWrtSystemTime(-11);
			String 	processEndTime = instanceUtil.getTimeWrtSystemTime(15);


			process = instanceUtil.setProcessCluster(process,null,XmlUtil.createProcessValidity
			(startTime,
			"2099-01-01T00:00Z"));
			process = instanceUtil.setProcessCluster(process,Util.readClusterName(b1.getClusters()
			.get(0)),
			XmlUtil.createProcessValidity(processStartTime,processEndTime));
			process = instanceUtil.setProcessCluster(process,Util.readClusterName(b3.getClusters()
			.get(0)),
			XmlUtil.createProcessValidity(processStartTime,processEndTime));
			process = instanceUtil.addProcessInputFeed(process,Util.readDatasetName(feed02),
			Util.readDatasetName(feed02));


			//submit and schedule process
			Util.print("process: "+process);

			r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
			process);

			Util.print("Wait till process goes into running ");

			for(int i = 0 ; i < 30 ; i++)
			{
				Status sUa1 = 	instanceUtil.getInstanceStatus(ua1, Util.getProcessName(process),
				0, 0);
				Status sUa2 =	instanceUtil.getInstanceStatus(ua3, Util.getProcessName(process),
				0, 0);
				if((sUa1.toString().equals("RUNNING") || (sUa1.toString().equals("SUCCEEDED ")))
				 &&  (sUa2.toString
				().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED")) )
					break;
				Thread.sleep(20000);
				
			}


			

			ProcessInstancesResult responseInstance  =  prismHelper.getProcessHelper()
			.getProcessInstanceStatus(Util
			.readEntityName(b1.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil
			.addMinsToTime
			(processStartTime, 7));
	//		Util.assertSucceeded(responseInstance);
			
			responseInstance  =  ua1.getProcessHelper().getProcessInstanceSuspend(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime, 7));
		//	Util.assertSucceeded(responseInstance);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime
			(processStartTime, 7));
//			Util.assertSucceeded(responseInstance);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceResume(Util
			.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime(processStartTime, 7));
	//		Util.assertSucceeded(responseInstance);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime(processStartTime, 7));
		//	Util.assertSucceeded(responseInstance);

			responseInstance  =  ua1.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b1.getProcessData()
			),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime(processStartTime, 7));
			//Util.assertSucceeded(responseInstance);

			responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b1
			.getProcessData()),"?start="+processStartTime+"&end="+instanceUtil.addMinsToTime(processStartTime, 7));
			//Util.assertSucceeded(responseInstance);

		}
		finally
		{
			b1.deleteBundle(prismHelper);
			b2.deleteBundle(prismHelper);
			b3.deleteBundle(prismHelper);

		}
	}
	
	*/
}

