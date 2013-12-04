/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.prism;

import java.lang.reflect.Method;

import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.util.Util;

/**
 *
 * @author rishu.mehrotra
 */
public class PrismFeedSuspendTest {
	
	
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	

    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendDeletedFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule using colohelpers
        //submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
        //submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);
        
        submitAndScheduleFeed(UA1Bundle);
        submitAndScheduleFeed(UA2Bundle);
        
        //delete using prismHelper
        Util.assertSucceeded(prismHelper.getFeedHelper().delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));


        //suspend using prismHelper
        Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        //verify
        Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "KILLED", UA1ColoHelper).get(0).contains("KILLED"));
        Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "RUNNING", UA2ColoHelper).get(0).contains("RUNNING"));


        Util.assertSucceeded(prismHelper.getFeedHelper().delete(Util.URLS.DELETE_URL, UA2Bundle.getDataSets().get(0)));
        //suspend on the other one
        Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
        Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "KILLED", UA1ColoHelper).get(0).contains("KILLED"));
        Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "KILLED", UA2ColoHelper).get(0).contains("KILLED"));
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule using colohelpers
        submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
        submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);


        for (int i = 0; i < 2; i++) {
            //suspend using prismHelper
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "SUSPENDED", UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "RUNNING", UA2ColoHelper).get(0).contains("RUNNING"));
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "SUSPENDED", UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "SUSPENDED", UA2ColoHelper).get(0).contains("SUSPENDED"));
        }
    }


    @Test(dataProvider = "DP")
    public void testSuspendNonExistentFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));

        Util.assertFailed(UA1ColoHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(UA2ColoHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
    }

    @Test(dataProvider = "DP")
    public void testSuspendSubmittedFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        submitFeed(UA1Bundle);
        submitFeed(UA2Bundle);

        Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));

        Util.assertFailed(UA1ColoHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(UA2ColoHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));


    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendScheduledFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);
            
            
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //suspend using prismHelper
            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "RUNNING", UA2ColoHelper).get(0).contains("RUNNING"));

            //suspend on the other one
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "SUSPENDED", UA2ColoHelper).get(0).contains("SUSPENDED"));
            ///Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "RUNNING", UA1ColoHelper).get(0).contains("RUNNING"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendDeletedFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);

            //delete using coloHelpers
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //suspend using prismHelper
            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "KILLED", UA1ColoHelper).get(0).contains("KILLED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "RUNNING", UA2ColoHelper).get(0).contains("RUNNING"));


            Util.assertSucceeded(prismHelper.getFeedHelper().delete(Util.URLS.DELETE_URL, UA2Bundle.getDataSets().get(0)));
            //suspend on the other one
            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "KILLED", UA1ColoHelper).get(0).contains("KILLED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "KILLED", UA2ColoHelper).get(0).contains("KILLED"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getFeedHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);



            //suspend using prismHelper
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "SUSPENDED", UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "RUNNING", UA2ColoHelper).get(0).contains("RUNNING"));

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));


            //for (int i = 0; i < 2; i++) 
            {
                //suspend on the other one
                Util.assertSucceeded(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
                Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)), "SUSPENDED", UA1ColoHelper).get(0).contains("SUSPENDED"));
                Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)), "SUSPENDED", UA2ColoHelper).get(0).contains("SUSPENDED"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendNonExistentFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));

            Util.assertFailed(UA2ColoHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitFeed(UA1Bundle);
            submitFeed(UA2Bundle);

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            Util.assertFailed(prismHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));


            Util.assertFailed(UA2ColoHelper.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }

    }
  

    private void submitFeed(Bundle bundle) throws Exception {
        
        for(String cluster:bundle.getClusters())
        {
            Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL, bundle.getDataSets().get(0)));
    }

    
    private void submitAndScheduleFeedUsingColoHelper(ColoHelper coloHelper, Bundle bundle) throws Exception {
        submitFeed(bundle);
        Util.assertSucceeded(coloHelper.getFeedHelper().schedule(Util.URLS.SCHEDULE_URL, bundle.getDataSets().get(0)));
    }
    
    private void submitAndScheduleFeed(Bundle bundle) throws Exception
    {
        for(String cluster:bundle.getClusters())
        {
            Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,cluster));
        }
        Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,bundle.getDataSets().get(0)));
    }

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        //return Util.readBundles("src/test/resources/LateDataBundles");
        return Util.readELBundles();
    }
}
