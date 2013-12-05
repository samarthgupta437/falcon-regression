/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.prism;



import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
public class PrismClusterDeleteTest {
	
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	
    
        PrismHelper prismHelper=new PrismHelper("prism.properties");
        ColoHelper UA1ColoHelper=new ColoHelper("mk-qa.config.properties");
        ColoHelper UA2ColoHelper=new ColoHelper("ivoryqa-1.config.properties");
        
    @Test(dataProvider="DP",groups = {"multiCluster"})
    public void testUA1ClusterDeleteInBothColos(Bundle bundle) throws Exception
    {
        //create a UA1 bundle
        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();
        
        //now submit the thing to prism
        Util.assertSucceeded((prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,UA1Bundle.getClusters().get(0))));
        
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the initial store and archive for both colos
        List<String> initialUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> initialUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //now lets get the final states
        List<String> finalPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the final store and archive for both colos
        List<String> finalUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> finalUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //now ensure that data has been deleted from all cluster store and is present in the cluster archives
        
        String clusterName=Util.readClusterName(bundle.getClusters().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore,finalPrismStore,clusterName);
        compareDataStoreStates(finalPrismArchiveStore,initialPrismArchiveStore, clusterName);
        
        //UA1:
        compareDataStoreStates(initialUA1Store,finalUA1Store, clusterName);
        compareDataStoreStates(finalUA1ArchiveStore,initialUA1ArchiveStore, clusterName);
        
        //UA2:
        compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
        compareDataStoreStates(finalUA2ArchiveStore,initialUA2ArchiveStore, clusterName);
        

    }
    
    @Test(dataProvider="DP",groups = {"multiCluster"})
    public void testUA1ClusterDeleteWhen1ColoIsDown(Bundle bundle) throws Exception
    {
        try{
        //create a UA1 bundle
        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();
        
        //now submit the thing to prism
        
       
       Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,UA1Bundle.getClusters().get(0)));
        
        
        
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the initial store and archive for both colos
        List<String> initialUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> initialUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        
        //bring down UA1 colo :P
        Util.shutDownService(UA1ColoHelper.getClusterHelper());
        
        //lets now delete the cluster from both colos
        Util.assertPartialSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        
        //now lets get the final states
        List<String> finalPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the final store and archive for both colos
        List<String> finalUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> finalUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //now ensure that data has been deleted from all cluster store and is present in the cluster archives
        
        String clusterName=Util.readClusterName(bundle.getClusters().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore,finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore,initialPrismArchiveStore);
        
        //UA2:
        compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
        compareDataStoreStates(finalUA2ArchiveStore,initialUA2ArchiveStore, clusterName);
        
        //UA1:
        Assert.assertTrue(Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),finalUA1Store.toArray(new String[finalUA1Store.size()])));
        
        //now bring up the service and roll forward the delete
        Util.startService(UA1ColoHelper.getClusterHelper());
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //get final data states:
        List<String> UA1ArchiveFinalState2=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        List<String> UA1StoreFinalState2=UA1ColoHelper.getClusterHelper().getStoreInfo();
        
        List<String> prismStorePostUp=prismHelper.getClusterHelper().getStoreInfo();
        List<String> prismArchivePostUp=prismHelper.getClusterHelper().getArchiveInfo();
        
        compareDataStoreStates(finalUA1Store,UA1StoreFinalState2, clusterName);
        compareDataStoreStates(UA1ArchiveFinalState2,finalUA1ArchiveStore,clusterName);
        
        List<String> UA2ArchiveStateFinal=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        List<String> UA2StoreStateFinal=UA2ColoHelper.getClusterHelper().getStoreInfo();
        
        compareDataStoreStates(UA2ArchiveStateFinal,initialUA2ArchiveStore, clusterName);
        compareDataStoreStates(initialUA2Store,UA2StoreStateFinal, clusterName);
        
        compareDataStoreStates(finalPrismStore,prismStorePostUp, clusterName);
        compareDataStoreStates(prismArchivePostUp,finalPrismArchiveStore, clusterName);
        
        
        
        
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }
    
    
        
    @Test(dataProvider="DP",groups = {"multiCluster"})
    public void testUA1ClusterDeleteAlreadyDeletedCluster(Bundle bundle) throws Exception
    {
        try{
        //create a UA1 bundle
        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();
        
        //now submit the thing to prism
        Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,UA1Bundle.getClusters().get(0)));
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the initial store and archive for both colos
        List<String> initialUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> initialUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //now lets get the final states
        List<String> finalPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the final store and archive for both colos
        List<String> finalUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> finalUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //now ensure that data has been deleted from all cluster store and is present in the cluster archives
        
        String clusterName=Util.readClusterName(bundle.getClusters().get(0));
        //prism:
        Assert.assertTrue(Arrays.deepEquals(initialPrismStore.toArray(new String[initialPrismStore.size()]),finalPrismStore.toArray(new String[finalPrismStore.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialPrismArchiveStore.toArray(new String[initialPrismArchiveStore.size()]),finalPrismArchiveStore.toArray(new String[finalPrismArchiveStore.size()])));
        //UA2:
        Assert.assertTrue(Arrays.deepEquals(initialUA2Store.toArray(new String[initialUA2Store.size()]),finalUA2Store.toArray(new String[finalUA2Store.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialUA2ArchiveStore.toArray(new String[initialUA2ArchiveStore.size()]),finalUA2ArchiveStore.toArray(new String[finalUA2ArchiveStore.size()])));
        //UA1:
        Assert.assertTrue(Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),finalUA1Store.toArray(new String[finalUA1Store.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialUA1ArchiveStore.toArray(new String[initialUA1ArchiveStore.size()]),finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));
        
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }         
        
    
    
    @Test(dataProvider="DP",groups = {"multiCluster"})
    public void testUA1ClusterDeleteTwiceWhen1ColoIsDownDuring1stDelete(Bundle bundle) throws Exception
    {
        try{
        //create a UA1 bundle
        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();
        
        Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,UA1Bundle.getClusters().get(0)));
        
        Util.shutDownService(UA1ColoHelper.getClusterHelper());
        
        
        
        //lets now delete the cluster from both colos
        Util.assertPartialSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //now lets get the final states
        List<String> initialPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the initial store and archive for both colos
        List<String> initialUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> initialUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //start up service
        Util.startService(UA1ColoHelper.getClusterHelper());
        
        //delete again
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //get final states
        List<String> finalPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the final store and archive for both colos
        List<String> finalUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> finalUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //now ensure that data has been deleted from all cluster store and is present in the cluster archives
        
        String clusterName=Util.readClusterName(bundle.getClusters().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore,initialPrismArchiveStore, clusterName);
        
        //Assert.assertTrue(Arrays.deepEquals(initialPrismStore.toArray(new String[initialPrismStore.size()]),finalPrismStore.toArray(new String[finalPrismStore.size()])));
        //Assert.assertTrue(Arrays.deepEquals(initialPrismArchiveStore.toArray(new String[initialPrismArchiveStore.size()]),finalPrismArchiveStore.toArray(new String[finalPrismArchiveStore.size()])));
        
        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(initialUA2ArchiveStore,finalUA2ArchiveStore);
        
        
        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, clusterName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, clusterName);
        
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally {
            
            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    } 

    @Test(dataProvider="DP",groups = {"multiCluster"})
    public void testUA1ClusterDeleteNonExistent(Bundle bundle) throws Exception
    {
        try{
        //create a UA1 bundle
        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();
        
        
        //now lets get the final states
        List<String> initialPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the initial store and archive for both colos
        List<String> initialUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> initialUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
       
        //delete
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //get final states
        List<String> finalPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the final store and archive for both colos
        List<String> finalUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> finalUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //now ensure that data has been deleted from all cluster store and is present in the cluster archives
        
        String clusterName=Util.readClusterName(bundle.getClusters().get(0));
        //prism:
        Assert.assertTrue(Arrays.deepEquals(initialPrismStore.toArray(new String[initialPrismStore.size()]),finalPrismStore.toArray(new String[finalPrismStore.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialPrismArchiveStore.toArray(new String[initialPrismArchiveStore.size()]),finalPrismArchiveStore.toArray(new String[finalPrismArchiveStore.size()])));
        
        //UA2:
        Assert.assertTrue(Arrays.deepEquals(initialUA2Store.toArray(new String[initialUA2Store.size()]),finalUA2Store.toArray(new String[finalUA2Store.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialUA2ArchiveStore.toArray(new String[initialUA2ArchiveStore.size()]),finalUA2ArchiveStore.toArray(new String[finalUA2ArchiveStore.size()])));
        
        //UA1:
        Assert.assertTrue(Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),finalUA1Store.toArray(new String[finalUA1Store.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialUA1ArchiveStore.toArray(new String[initialUA1ArchiveStore.size()]),finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));
        
        
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }

    } 
        

    @Test(dataProvider="DP",groups = {"multiCluster"})
    public void testUA1ClusterDeleteNonExistentWhen1ColoIsDownDuringDelete(Bundle bundle) throws Exception
    {
        try{
        //create a UA1 bundle
        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();
        
        
        //now lets get the final states
        List<String> initialPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the initial store and archive for both colos
        List<String> initialUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> initialUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //bring down UA1
        Util.shutDownService(UA1ColoHelper.getClusterHelper());
       
        //delete
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        //get final states
        List<String> finalPrismStore=prismHelper.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore=prismHelper.getClusterHelper().getArchiveInfo();
        
        //fetch the final store and archive for both colos
        List<String> finalUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> finalUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        //now ensure that data has been deleted from all cluster store and is present in the cluster archives
        
        String clusterName=Util.readClusterName(bundle.getClusters().get(0));
        //prism:
        Assert.assertTrue(Arrays.deepEquals(initialPrismStore.toArray(new String[initialPrismStore.size()]),finalPrismStore.toArray(new String[finalPrismStore.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialPrismArchiveStore.toArray(new String[initialPrismArchiveStore.size()]),finalPrismArchiveStore.toArray(new String[finalPrismArchiveStore.size()])));
        
        //UA2:
        Assert.assertTrue(Arrays.deepEquals(initialUA2Store.toArray(new String[initialUA2Store.size()]),finalUA2Store.toArray(new String[finalUA2Store.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialUA2ArchiveStore.toArray(new String[initialUA2ArchiveStore.size()]),finalUA2ArchiveStore.toArray(new String[finalUA2ArchiveStore.size()])));
        
        //UA1:
        Assert.assertTrue(Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),finalUA1Store.toArray(new String[finalUA1Store.size()])));
        Assert.assertTrue(Arrays.deepEquals(initialUA1ArchiveStore.toArray(new String[initialUA1ArchiveStore.size()]),finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));
        
        Util.startService(UA1ColoHelper.getFeedHelper());
        Util.assertSucceeded(prismHelper.getClusterHelper().delete(Util.URLS.DELETE_URL,UA1Bundle.getClusters().get(0)));
        
        List<String> UA1StorePostUp=UA1ColoHelper.getClusterHelper().getStoreInfo();
        List<String> UA1ArchivePostUp=UA1ColoHelper.getClusterHelper().getArchiveInfo();
        
        List<String> UA2StorePostUp=UA2ColoHelper.getClusterHelper().getStoreInfo();
        List<String> UA2ArchivePostUp=UA2ColoHelper.getClusterHelper().getArchiveInfo();
        
        Assert.assertTrue(Arrays.deepEquals(UA2StorePostUp.toArray(new String[UA2StorePostUp.size()]),finalUA2Store.toArray(new String[finalUA2Store.size()])));
        Assert.assertTrue(Arrays.deepEquals(UA2ArchivePostUp.toArray(new String[UA2ArchivePostUp.size()]),finalUA2ArchiveStore.toArray(new String[finalUA2ArchiveStore.size()])));
        
        compareDataStoresForEquality(finalUA1Store,UA1StorePostUp);
        compareDataStoresForEquality(UA1ArchivePostUp,finalUA1ArchiveStore);
        
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        finally{
            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }                 
        
        
    
    
    @DataProvider(name="DP")
    public Object[][] getData() throws Exception
    {
        return Util.readBundles("src/test/resources/LateDataBundles");
    }
    
    private void compareDataStoreStates(List<String> initialState,List<String> finalState,String filename) throws Exception
    {
       initialState.removeAll(finalState);
       Assert.assertEquals(initialState.size(),1);
       Assert.assertTrue(initialState.get(0).contains(filename));
        
    }
    
    private void compareDataStoresForEquality(List<String> store1,List<String> store2) throws Exception
    {
        Assert.assertTrue(Arrays.deepEquals(store2.toArray(new String[store2.size()]), store1.toArray(new String[store1.size()])));
    }
    
}
