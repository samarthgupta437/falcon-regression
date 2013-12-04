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
import com.inmobi.qa.falcon.generated.feed.ActionType;
import com.inmobi.qa.falcon.generated.feed.ClusterType;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.APIResult;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.supportClasses.GetBundle;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;
import com.inmobi.qa.falcon.util.instanceUtil;
import com.inmobi.qa.falcon.util.xmlUtil;

/**
 *
 * @author rishu.mehrotra
 */
public class PrismFeedSnSTest {
    
	
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	
	
        PrismHelper prismHelper=new PrismHelper("prism.properties");
        ColoHelper UA1ColoHelper=new ColoHelper("mk-qa.config.properties");
        ColoHelper UA2ColoHelper=new ColoHelper("ivoryqa-1.config.properties");
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOnBothColos(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            //schedule both bundles
            submitAndScheduleFeed(UA1Bundle);
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0).contains("RUNNING"));
            Assert.assertEquals(Util.getOozieJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0),"No Jobs match your criteria!");
            
            
            submitAndScheduleFeed(UA2Bundle);
            
            //now check if they have been scheduled correctly or not
            
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            
            //check if there is no criss cross
            
            Assert.assertEquals(Util.getOozieJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            
        }
      
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testSnSAlreadyScheduledFeedOnBothColos(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            //schedule both bundles
            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);
            
            //now check if they have been scheduled correctly or not
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0).contains("RUNNING"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            
            //check if there is no criss cross
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0),"No Jobs match your criteria!");
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            
            
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            //ensure only one bundle is there
            Assert.assertEquals(Util.getBundles(UA1ColoHelper,Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"feed").size(),1);
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            Assert.assertEquals(Util.getBundles(UA2ColoHelper,Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"feed").size(),1);
            //now check if they have been scheduled correctly or not
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0).contains("RUNNING"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
                        
            
        }
        
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testSnSSuspendedFeedOnBothColos(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            //schedule both bundles
            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);
            
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            //now check if they have been scheduled correctly or not
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertEquals(Util.getBundles(UA1ColoHelper,Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"feed").size(),1);

            Util.assertSucceeded(UA1ColoHelper.getFeedHelper().resume(URLS.RESUME_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0).contains("RUNNING"));
                        
            
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL,UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"SUSPENDED",UA2ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0).contains("RUNNING"));
            
            
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"SUSPENDED",UA2ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertEquals(Util.getBundles(UA2ColoHelper,Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"feed").size(),1);
            Util.assertSucceeded(UA2ColoHelper.getFeedHelper().resume(URLS.RESUME_URL,UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            
            
           
        }        
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testSnSDeletedFeedOnBothColos(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            //schedule both bundles
            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);
            
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL,UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"KILLED",UA2ColoHelper).get(0).contains("KILLED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            
        } 
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testScheduleNonExistentFeedOnBothColos(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            Util.assertFailed(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            Util.assertFailed(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));

        } 
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileOtherColoIsDown(Bundle bundle) throws Exception
        {
            try{
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            for(String cluster:UA2Bundle.getClusters())
            {
                Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,cluster));
            }
            
            Util.shutDownService(UA1ColoHelper.getFeedHelper());
            
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            //submitAndScheduleFeed(UA2Bundle);
            
            //now check if they have been scheduled correctly or not
            
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            
            //check if there is no criss cross
            
            Assert.assertEquals(Util.getOozieJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            }
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            finally{
                
                Util.restartService(UA1ColoHelper.getFeedHelper());
                
            }
            
        }  
        

        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileThatColoIsDown(Bundle bundle) throws Exception
        {
            try{
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            submitFeed(UA1Bundle);
            
            Util.shutDownService(UA1ColoHelper.getFeedHelper());
            
            Util.assertFailed(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertEquals(Util.getOozieJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            }
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            finally{
                
                Util.restartService(UA1ColoHelper.getFeedHelper());
                
            }
            
        }         
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeed(Bundle bundle) throws Exception
        {
            try{
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            submitAndScheduleFeed(UA1Bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            
            submitAndScheduleFeed(UA2Bundle);
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0),"No Jobs match your criteria!");
            
            }
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            
        }
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeed(Bundle bundle) throws Exception
        {
            try{
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            submitAndScheduleFeed(UA1Bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            
            submitAndScheduleFeed(UA2Bundle);
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0),"No Jobs match your criteria!");
            
            }
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            
        } 
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOnBothColosUsingColoHelper(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            //schedule both bundles
            submitFeed(UA1Bundle);
            APIResult result=Util.parseResponse((UA1ColoHelper.getFeedHelper().submitEntity(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0))));
            Assert.assertEquals(result.getStatusCode(),404);
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0),"No Jobs match your criteria!");
            
            
            submitFeed(UA2Bundle);
            result=Util.parseResponse(UA2ColoHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            Assert.assertEquals(result.getStatusCode(),404);
            //now check if they have been scheduled correctly or not
            
            
            
            //check if there is no criss cross
            
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            
        }
        
        
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testSnSSuspendedFeedOnBothColosUsingColoHelper(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            //schedule both bundles
            submitFeed(UA1Bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            submitFeed(UA2Bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            
            Util.assertSucceeded(UA1ColoHelper.getFeedHelper().suspend(URLS.SUSPEND_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            //now check if they have been scheduled correctly or not
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            Util.assertSucceeded(UA1ColoHelper.getFeedHelper().resume(URLS.RESUME_URL,UA1Bundle.getDataSets().get(0)));
            
            Util.assertSucceeded(UA2ColoHelper.getFeedHelper().suspend(URLS.SUSPEND_URL,UA2Bundle.getDataSets().get(0)));
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"SUSPENDED",UA2ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0).contains("RUNNING"));
           
        } 
        
       
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testScheduleDeletedFeedOnBothColosUsingColoHelper(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            //schedule both bundles
            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);
            
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL,UA2Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"KILLED",UA2ColoHelper).get(0).contains("KILLED"));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0)));
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            
            Assert.assertEquals(Util.parseResponse(prismHelper.getFeedHelper().getStatus(URLS.STATUS_URL,UA1Bundle.getDataSets().get(0))).getMessage(),"ua1/RUNNING");
            Assert.assertEquals(Util.parseResponse(prismHelper.getFeedHelper().getStatus(URLS.STATUS_URL,UA2Bundle.getDataSets().get(0))).getMessage(),"ua2/RUNNING");
            
        }
       
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testSNSNonExistentFeedOnBothColosUsingColoHelper(Bundle bundle) throws Exception
        {
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            Assert.assertEquals(Util.parseResponse(UA1ColoHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA1Bundle.getDataSets().get(0))).getStatusCode(),404);
            Assert.assertEquals(Util.parseResponse(UA2ColoHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0))).getStatusCode(),404);
        }
        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileOtherColoIsDownUsingColoHelper(Bundle bundle) throws Exception
        {
            try{
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            for(String cluster:UA2Bundle.getClusters())
            {
                Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,cluster));
            }
            
            Util.shutDownService(UA1ColoHelper.getFeedHelper());
            
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,UA2Bundle.getDataSets().get(0)));
            //submitAndScheduleFeedUsingColoHelper(UA2ColoHelper,UA2Bundle);
            
            //now check if they have been scheduled correctly or not
            
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            
            //check if there is no criss cross
            
            Assert.assertEquals(Util.getOozieJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            }
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            finally{
                
                Util.restartService(UA1ColoHelper.getFeedHelper());
                
            }
            
        }

        
 
       @Test(groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileThatColoIsDownUsingColoHelper() throws Exception
        {
        	Bundle b1 = (Bundle)Util.readELBundles()[0][0];
    		b1.generateUniqueBundle();
    		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
    		b2.generateUniqueBundle();
    		
    		try{
    			b1 = new Bundle(b1,UA1ColoHelper.getEnvFileName());
    			b2  = new Bundle(b2,UA2ColoHelper.getEnvFileName());
    		
    			b1.setCLusterColo("ua1");
    			Util.print("cluster b1: "+b1.getClusters().get(0));

    			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b1.getClusters().get(0));
    			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

    			b2.setCLusterColo("ua2");
    			Util.print("cluster b2: "+b2.getClusters().get(0));
    			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b2.getClusters().get(0));
    			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

    			String startTimeUA1 = "2012-10-01T12:00Z" ;
    			String startTimeUA2 = "2012-10-01T12:00Z";
    			
    			String feed = b1.getDataSets().get(0);
    			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
    		    feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2099-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
    			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2099-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,null,"/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
    		
    		 	Util.print("feed: "+feed);
            
    			Util.shutDownService(UA1ColoHelper.getFeedHelper());
    	        
    			ServiceResponse response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL,feed);
    			Util.assertPartialSucceeded(response);
    			//Util.assertSucceeded(response);
    			response=prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
    			Util.assertPartialSucceeded(response);
    		
    		}
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            finally {
                
                Util.restartService(UA1ColoHelper.getFeedHelper());
                prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
                prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
            }

        }       

        
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeedUsingColoHelper(Bundle bundle) throws Exception
        {
            try{
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            submitAndScheduleFeed(UA1Bundle);
            Util.assertSucceeded(UA1Bundle.getFeedHelper().suspend(URLS.SUSPEND_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            
            submitAndScheduleFeed(UA2Bundle);
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            Assert.assertEquals(Util.getOozieJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"SUSPENDED",UA1ColoHelper).get(0).contains("SUSPENDED"));
            Assert.assertEquals(Util.getOozieJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0),"No Jobs match your criteria!");
            
            }
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            
        }
        
   
        @Test(dataProvider="DP",groups={"prism","0.2"})
        public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeedUsingColoHelper(Bundle bundle) throws Exception
        {
            try{
            Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle=new Bundle(bundle,UA2ColoHelper.getEnvFileName());
            
            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();
            
            submitAndScheduleFeed(UA1Bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL,UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            
            submitAndScheduleFeed(UA2Bundle);
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0).contains("RUNNING"));
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"RUNNING",UA2ColoHelper).get(0),"No Jobs match your criteria!");
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(UA1Bundle.getDataSets().get(0)),"KILLED",UA1ColoHelper).get(0).contains("KILLED"));
            Assert.assertEquals(Util.getOozieFeedJobStatus(Util.readDatasetName(UA2Bundle.getDataSets().get(0)),"RUNNING",UA1ColoHelper).get(0),"No Jobs match your criteria!");
            
            }
            catch(Exception e)
            {
                e.printStackTrace();
                throw new TestNGException(e.getMessage());
            }
            
        }                                             
   
    private void submitFeed(Bundle bundle) throws Exception
    {
        for(String cluster:bundle.getClusters())
        {
            Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,cluster));
        }
        
        
        Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL,bundle.getDataSets().get(0)));
    }        
       
        
    private void submitAndScheduleFeed(Bundle bundle) throws Exception
    {
        for(String cluster:bundle.getClusters())
        {
            Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,cluster));
        }
        Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,bundle.getDataSets().get(0)));
    }
    
    private void submitAndScheduleFeedUsingColoHelper(ColoHelper coloHelper,Bundle bundle) throws Exception
    {
        for(String cluster:bundle.getClusters())
        {
            Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,cluster));
        }
        Util.assertSucceeded(coloHelper.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,bundle.getDataSets().get(0)));
    }    
     
  
    
    @DataProvider(name="DP")
    public Object[][] getData() throws Exception
    {
        //return Util.readBundles("src/test/resources/LateDataBundles");
    	return Util.readELBundles();
    }     
    
}
