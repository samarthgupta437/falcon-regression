/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.helpers;

import java.util.ArrayList;
import java.util.List;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.interfaces.EntityHelperFactory;
import com.inmobi.qa.falcon.interfaces.IEntityManagerHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;

/**
 *
 * @author rishu.mehrotra
 */
public class PrismHelper {
    
    protected IEntityManagerHelper clusterHelper;
    protected IEntityManagerHelper processHelper;
    protected IEntityManagerHelper feedHelper;
    protected com.inmobi.qa.falcon.util.instanceUtil instanceUtil;

    public com.inmobi.qa.falcon.util.instanceUtil getInstanceUtil() {
        return instanceUtil;
    }

    public void setInstanceUtil(com.inmobi.qa.falcon.util.instanceUtil instanceUtil) {
        this.instanceUtil = instanceUtil;
    }

    public Util getUtil() {
        return util;
    }

    public void setUtil(Util util) {
        this.util = util;
    }
    protected Util util;

    public IEntityManagerHelper getClusterHelper() {
        return clusterHelper;
    }

    public IEntityManagerHelper getFeedHelper() {
        return feedHelper;
    }

    public IEntityManagerHelper getProcessHelper() {
        return processHelper;
    }
    protected String envFileName;

    public String getEnvFileName() {
        return envFileName;
    }
    
    public PrismHelper(String envFileName)
    {
        try {
        this.envFileName=envFileName;
        clusterHelper= EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER, this.envFileName);
        processHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS, this.envFileName);
        feedHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA, this.envFileName);
        instanceUtil=new instanceUtil(this.envFileName);
        
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        
    }
    
    public ServiceResponse submitCluster(Bundle bundle) throws Exception
    {
        return clusterHelper.submitEntity(Util.URLS.SUBMIT_URL,bundle.getClusterData());
    }
    
    public List<ServiceResponse> submitFeed(Bundle bundle) throws Exception
    {
       List<ServiceResponse> responseList=new ArrayList<ServiceResponse>();
       for(String feed:bundle.getDataSets())
       {
           responseList.add(feedHelper.submitEntity(Util.URLS.SUBMIT_URL,feed));
       }
       return responseList;
    }
    
    public ServiceResponse submitProcess(Bundle bundle) throws Exception
    {
        return processHelper.submitEntity(Util.URLS.SUBMIT_URL,bundle.getProcessData());
    }
    
    public ServiceResponse scheduleProcess(Bundle bundle) throws Exception
    {
        return processHelper.schedule(Util.URLS.SCHEDULE_URL,Util.readEntityName(bundle.getProcessData()));
    }
    
    
    
}
