/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon;

import java.lang.reflect.Method;

import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.util.AssertUtil;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;
/**
 * 
 * @author samarth.gupta
 *
 */
public class FeedScheduleTest {
	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");
	
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	
	
    @Test(groups = {"singleCluster"})
    public void scheduleAlreadyScheduledFeed()
    {
    	Bundle bundle = new Bundle();
        try
        {
        	
        	bundle = (Bundle)Util.readELBundles()[0][0];
        	bundle  = new Bundle(bundle,ivoryqa1.getEnvFileName());
        	bundle.submitCluster(bundle);
        	String feed=Util.getInputFeedFromBundle(bundle);
            
            ServiceResponse response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Util.assertSucceeded(response);
            
            response= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Util.assertSucceeded(response);
            
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed),"RUNNING",ivoryqa1).get(0).contains("RUNNING"));
            
            //now try re-scheduling again
            response= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            AssertUtil.assertSucceeded(response);
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
         
            try {
                
               prismHelper.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
            }
            catch(Exception e){}
        }
    }
    
    
   @Test(groups = {"singleCluster"})
    public void scheduleValidFeed() throws Exception
    {
    	Bundle bundle = new Bundle();
        try
        {
        	
        	bundle = (Bundle)Util.readELBundles()[0][0];
        	bundle  = new Bundle(bundle,ivoryqa1.getEnvFileName());
        	bundle.submitCluster(bundle);
            String feed=Util.getInputFeedFromBundle(bundle);
            //submit feed
            ServiceResponse response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Util.assertSucceeded(response);
            //now schedule the thing
            response=prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL,feed);
            Util.assertSucceeded(response);
            
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed),"RUNNING",ivoryqa1).get(0).contains("RUNNING"));
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
          
                prismHelper.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
            }
            
    }
    
  
    
    @Test(groups = {"singleCluster"})
    public void scheduleSuspendedFeed() throws Exception
    {
    	Bundle bundle = new Bundle();
        try
        {
        	
        	bundle = (Bundle)Util.readELBundles()[0][0];
        	bundle  = new Bundle(bundle,ivoryqa1.getEnvFileName());
        	bundle.submitCluster(bundle);
            String feed=Util.getInputFeedFromBundle(bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
            
            //now suspend
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL, feed));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed),"SUSPENDED",ivoryqa1).get(0).contains("SUSPENDED"));
            
            //now schedule this!
            Util.assertSucceeded(prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed),"SUSPENDED",ivoryqa1).get(0).contains("SUSPENDED"));
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            prismHelper.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));

        }
    }
    
    @Test(groups = {"singleCluster"})
    public void scheduleKilledFeed() throws Exception
    {
    	Bundle bundle = new Bundle();
        try
        {
        	
        	bundle = (Bundle)Util.readELBundles()[0][0];
        	bundle  = new Bundle(bundle,ivoryqa1.getEnvFileName());
        	bundle.submitCluster(bundle);
            String feed=Util.getInputFeedFromBundle(bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
            
            //now suspend
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL, feed));
            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed),"KILLED",ivoryqa1).get(0).contains("KILLED"));
            
            //now schedule this!
            Util.assertFailed(prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
            prismHelper.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }
    
    @Test(groups = {"singleCluster"},dataProvider="DP",dataProviderClass=FeedSubmitTest.class)
    public void scheduleNonExistentFeed(Bundle bundle) throws Exception
    {
        bundle.generateUniqueBundle();
    	bundle.submitCluster(bundle);
        String feed=Util.getInputFeedFromBundle(bundle);
        Util.assertFailed(prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL,feed));
    }
    
    
      
    
}
