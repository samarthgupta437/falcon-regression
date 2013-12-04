/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon;


import java.lang.reflect.Method;

import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.interfaces.EntityHelperFactory;
import com.inmobi.qa.falcon.interfaces.IEntityManagerHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;

/**
 *
 * @author rishu.mehrotra
 */
public class FeedSubmitTest {
	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");        
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	
	
    IEntityManagerHelper clusterHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER);
    IEntityManagerHelper feedHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA);
    
    public void submitCluster(Bundle bundle) throws Exception
    {
        //submit the cluster
    	ServiceResponse response=prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(),200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }
    
    @Test(groups = {"singleCluster"},dataProvider="DP")
    public void submitValidFeed(Bundle bundle) throws Exception
    {
        try {
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle,ivoryqa1.getEnvFileName());
        submitCluster(bundle);
        //now submit an input dataset
        String feed=Util.getInputFeedFromBundle(bundle);
        
        ServiceResponse response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL,feed);
        Util.assertSucceeded(response);
        }
                catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
        	prismHelper.getFeedHelper().delete(URLS.DELETE_URL,Util.getInputFeedFromBundle(bundle));
        }
        
    }

    @Test(groups = {"singleCluster"},dataProvider="DP")
    public void submitValidFeedPostDeletion(Bundle bundle) throws Exception
    {
        try {
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle,ivoryqa1.getEnvFileName());
        submitCluster(bundle);
        //now submit an input dataset
        String feed=Util.getInputFeedFromBundle(bundle);
        
        ServiceResponse response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL,feed);
        Util.assertSucceeded(response);
        
        response=prismHelper.getFeedHelper().delete(URLS.DELETE_URL, feed);
        Util.assertSucceeded(response);
        
        response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        }
                catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
        	prismHelper.getFeedHelper().delete(URLS.DELETE_URL,Util.getInputFeedFromBundle(bundle));
        }
        
    }

    
    @Test(groups = {"singleCluster"},dataProvider="DP")
    public void submitValidFeedPostGet(Bundle bundle) throws Exception
    {
        try {
              bundle.generateUniqueBundle();
              bundle = new Bundle(bundle,ivoryqa1.getEnvFileName());
        submitCluster(bundle);
        //now submit an input dataset
        String feed=Util.getInputFeedFromBundle(bundle);
        
        ServiceResponse response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL,feed);
        Util.assertSucceeded(response);
        
        response=prismHelper.getFeedHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, feed);
        Util.assertSucceeded(response);
        
        response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed); 
        Util.assertSucceeded(response);
        }
                     catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
        	prismHelper.getFeedHelper().delete(URLS.DELETE_URL,Util.getInputFeedFromBundle(bundle));
        }
    }
   
    @Test(groups = {"singleCluster"},dataProvider="DP")
    public void submitValidFeedTwice(Bundle bundle) throws Exception
    {
        try {
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle,ivoryqa1.getEnvFileName()); 
        submitCluster(bundle);
        //now submit an input dataset
        String feed=Util.getInputFeedFromBundle(bundle);
        
        ServiceResponse response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL,feed);
        Util.assertSucceeded(response);
        
        response=prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed); 
        Util.assertSucceeded(response);
        }
                     catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
        	prismHelper.getFeedHelper().delete(URLS.DELETE_URL,Util.getInputFeedFromBundle(bundle));
        }
    }
    
   
    
    
    @DataProvider(name="DP")
    public static Object[][] getData(Method m) throws Exception
    {
        return Util.readELBundles();
    }
    
    
    
}
