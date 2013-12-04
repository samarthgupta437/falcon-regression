/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.interfaces;

import com.inmobi.qa.falcon.helpers.ClusterEntityHelperImpl;
import com.inmobi.qa.falcon.helpers.DataEntityHelperImpl;
import com.inmobi.qa.falcon.helpers.ProcessEntityHelperImpl;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;

/**
 *
 * @author rishu.mehrotra
 */
public class EntityHelperFactory {
    
   
    
    public static <T> IEntityManagerHelper getEntityHelper(ENTITY_TYPE type)
    {
       if(type.equals(ENTITY_TYPE.DATA))
       {
           return new DataEntityHelperImpl();
       }
       
       if(type.equals(ENTITY_TYPE.CLUSTER))
       {
           return new ClusterEntityHelperImpl();
       }
       
       if(type.equals(ENTITY_TYPE.PROCESS))
       {
           return new ProcessEntityHelperImpl();
       }
       
       return null;
    }
    
    public static <T> IEntityManagerHelper getEntityHelper(ENTITY_TYPE type,String envFileName) throws Exception
    {
       if(type.equals(ENTITY_TYPE.DATA))
       {
           return new DataEntityHelperImpl(envFileName);
       }
       
       if(type.equals(ENTITY_TYPE.CLUSTER))
       {
           return new ClusterEntityHelperImpl(envFileName);
       }
       
       if(type.equals(ENTITY_TYPE.PROCESS))
       {
           return new ProcessEntityHelperImpl(envFileName);
       }
       
       return null;
    }
}
