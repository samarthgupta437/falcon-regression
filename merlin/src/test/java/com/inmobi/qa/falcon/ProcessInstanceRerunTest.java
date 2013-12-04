package com.inmobi.qa.falcon;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ProcessInstancesResult;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;
/**
 * 
 * @author samarth.gupta
 *
 */
public class ProcessInstanceRerunTest {

    PrismHelper prismHelper=new PrismHelper("prism.properties");
    ColoHelper ivoryqa1 = new ColoHelper("ua4.properties");

    @BeforeClass(alwaysRun=true)
    public void createTestData() throws Exception
    {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = new Bundle();
        b = (Bundle)Util.readELBundles()[0][0];
        b  = new Bundle(b,ivoryqa1.getEnvFileName());
        b = new Bundle(b,ivoryqa1.getEnvFileName());

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(ivoryqa1,prefix.substring(1));

        DateTime startDateJoda = new DateTime(instanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(instanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,20);

        for(int i = 0 ; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for(int i = 0 ; i < dataDates.size(); i++)
            dataFolder.add(dataDates.get(i));

        instanceUtil.putDataInFolders(ivoryqa1,dataFolder);
    }



    @BeforeMethod(alwaysRun=true)
    public void testName(Method method)
    {
        Util.print("test name: "+method.getName());
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_someKilled02() throws Exception
    {

        Bundle b = new Bundle();

        try{
            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:26Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(5);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:16Z");
            instanceUtil.validateResponse(r, 4, 0, 0, 0,4);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            Thread.sleep(15000);
            instanceUtil.areWorkflowsRunning(ivoryqa1,Util.readEntityName(b.getProcessData()),6,5,1,0);
        }
        finally{
            b.deleteBundle(prismHelper);

        }
    }



    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleSucceededDeleted() throws Exception
    {

        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:11Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            //Thread.sleep(240000);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            Thread.sleep(15000);
            //instanceUtil.areWorkflowsRunning(Util.readEntityName(b.getProcessData()),3,3,0,0);
        }
        finally{
            b.deleteBundle(prismHelper);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");



        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleKilled() throws Exception
    {
        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:11Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(5);

            Util.print("process: "+b.getProcessData());

            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            instanceUtil.validateResponse(r, 3, 0, 0, 0,3);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            Thread.sleep(5000);
            instanceUtil.areWorkflowsRunning(ivoryqa1,Util.readEntityName(b.getProcessData()),3,3,0,0);
        }
        finally{
            b.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_someKilled01() throws Exception
    {
        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:26Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(5);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            instanceUtil.validateResponse(r, 3, 0, 0, 0,3);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            Thread.sleep(5000);
            instanceUtil.areWorkflowsRunning(ivoryqa1,Util.readEntityName(b.getProcessData()),6,6,0,0);
        }
        finally{
            b.deleteBundle(prismHelper);
        }
    }



    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_deleted() throws Exception
    {

        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:04Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/samarth/testProcessInstanceRerun_singleKilled/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(1);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
            Thread.sleep(15000);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
            Thread.sleep(15000);
            Assert.assertTrue(instanceUtil.isWorkflowRunning(instanceUtil.getWorkflows(ivoryqa1,Util.getProcessName(b.getProcessData()),Status.RUNNING).get(0)));

        }
        finally{
            b.deleteBundle(prismHelper);



        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleKilled() throws Exception
    {

        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:04Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/samarth/testProcessInstanceRerun_singleKilled/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(1);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(25000);
            prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
            Thread.sleep(25000);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
            Thread.sleep(25000);
            Assert.assertTrue(instanceUtil.isWorkflowRunning(instanceUtil.getWorkflows(ivoryqa1,Util.getProcessName(b.getProcessData()),Status.RUNNING).get(0)));

        }
        finally{
            b.deleteBundle(prismHelper);

        }
    }



    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleSucceeded() throws Exception
    {

        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:04Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/samarth/testProcessInstanceRerun_singleSucceeded/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(180000);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
            Thread.sleep(15000);
            Assert.assertTrue(instanceUtil.isWorkflowRunning(instanceUtil.getWorkflows(ivoryqa1,Util.getProcessName(b.getProcessData()),Status.RUNNING).get(0)));
        }
        finally{
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleSuspended() throws Exception
    {
        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:06Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/samarth/testProcessInstanceRerun_singleKilled/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(2);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper().getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:06Z");
            Thread.sleep(15000);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:06Z");
            Thread.sleep(15000);
            Assert.assertEquals(instanceUtil.getInstanceStatus(ivoryqa1,Util.getProcessName(b.getProcessData()),0,1),CoordinatorAction.Status.SUSPENDED);

        }
        finally{
            b.deleteBundle(prismHelper);
        }
    }





    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleSucceeded() throws Exception
    {
        Bundle b = new Bundle();

        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:11Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            //Thread.sleep(240000);
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            Thread.sleep(15000);
            instanceUtil.areWorkflowsRunning(ivoryqa1,Util.readEntityName(b.getProcessData()),3,3,0,0);
        }
        finally{
            b.deleteBundle(prismHelper);
        }
    }
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_timedOut() throws Exception
    {
        Bundle b = new Bundle();

        //submit 
        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/timedout/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:11Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setProcessTimeOut(2, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            //Thread.sleep(240000);
            org.apache.oozie.client.CoordinatorAction.Status s = null;
            while(!org.apache.oozie.client.CoordinatorAction.Status.TIMEDOUT.equals(s)){
                s = instanceUtil.getInstanceStatus(ivoryqa1, Util.readEntityName(b.getProcessData()), 0, 0);
                Thread.sleep(15000);
            }
            prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            Thread.sleep(15000);
            s = instanceUtil.getInstanceStatus(ivoryqa1, Util.readEntityName(b.getProcessData()), 0, 0);
            Assert.assertTrue(org.apache.oozie.client.CoordinatorAction.Status.WAITING.equals(s),"instance should have been in WAITING state");
        }
        finally{
            b.deleteBundle(prismHelper);
        }
    }


    @AfterClass(alwaysRun=true)
    public void deleteData() throws Exception
    {
        Util.print("in @AfterClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = new Bundle();
        b = (Bundle)Util.readELBundles()[0][0];
        b  = new Bundle(b,ivoryqa1.getEnvFileName());
        b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(ivoryqa1,prefix.substring(1));
    }

    
}
