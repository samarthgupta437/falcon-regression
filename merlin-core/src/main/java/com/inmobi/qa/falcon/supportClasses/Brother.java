/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.supportClasses;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.interfaces.IEntityManagerHelper;
import org.testng.TestNGException;
import org.testng.log4testng.Logger;

import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.interfaces.EntityHelperFactory;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.util.Util.URLS;

/**
 *
 * @author rishu.mehrotra
 */


public class Brother extends Thread {


	String operation;
	String data;
	String url;
	ServiceResponse output;
	URLS urls;
	private Logger logger=Logger.getLogger(this.getClass());

	public ServiceResponse getOutput() {
		return output;
	}
	IEntityManagerHelper entityManagerHelper;
	PrismHelper p;

	public Brother(String threadName,String operation,ENTITY_TYPE entityType,ThreadGroup tGroup,String data,URLS url)
	{
		super(tGroup,threadName);
		this.operation=new String(operation);      
		this.entityManagerHelper=EntityHelperFactory.getEntityHelper(entityType);
		this.data=new String(data);
		this.url=new String(url.getValue());
		this.urls=urls;
		this.output=new ServiceResponse();

	}


	public Brother(String threadName,String operation,ENTITY_TYPE entityType,ThreadGroup tGroup,Bundle b,PrismHelper p,URLS url)
	{
		super(tGroup,threadName);
		this.operation=new String(operation);      
		this.p=p;
		

		if(entityType.equals(ENTITY_TYPE.PROCESS)){
			this.data= b.getProcessData();
			this.entityManagerHelper = p.getProcessHelper();

		}
		else if(entityType.equals(ENTITY_TYPE.CLUSTER)){
			
			this.entityManagerHelper = p.getClusterHelper();
			this.data= b.getClusterData();
			}
		else{
			this.entityManagerHelper = p.getFeedHelper();
			this.data= b.getDataSets().get(0);
		}

		this.url= p.getClusterHelper().getHostname() + url.getValue();
		this.urls=urls;
		this.output=new ServiceResponse();

	}



	public String getData() {
		return data;
	}


	public void run()
	{
		try{this.sleep(50L);}catch(Exception e){e.printStackTrace();throw new TestNGException(e.getMessage());}


		//System.out.println("Brother "+this.getName()+" will be executing "+operation);
		logger.info("Brother "+this.getName()+" will be executing "+operation);

		try {


			if(operation.equalsIgnoreCase("submit"))
			{
				

				output=entityManagerHelper.submitEntity(url, data);

			}
			else if(operation.equalsIgnoreCase("get"))
			{
				output=entityManagerHelper.getEntityDefinition(url, data);
				//System.out.println("Brother "+this.getName()+"'s response to the "+operation+" is: "+output);
			}
			else if(operation.equalsIgnoreCase("delete"))
			{
				output=entityManagerHelper.delete(url, data);
				//System.out.println("Brother "+this.getName()+"'s response to the "+operation+" is: "+output);    
			}
			else if(operation.equalsIgnoreCase("suspend"))
			{
				output=entityManagerHelper.suspend(url, data);
				//System.out.println("Brother "+this.getName()+"'s response to the "+operation+" is: "+output);    
			}
			else if(operation.equalsIgnoreCase("schedule"))
			{
				output=entityManagerHelper.schedule(url, data);
				//System.out.println("Brother "+this.getName()+"'s response to the "+operation+" is: "+output);    
			}
			else if(operation.equalsIgnoreCase("resume"))
			{
				output=entityManagerHelper.resume(url, data);
			}

			else if(operation.equalsIgnoreCase("SnS"))
			{
				output=entityManagerHelper.submitAndSchedule(url, data);
			}

			else if(operation.equalsIgnoreCase("status"))
			{
				output=entityManagerHelper.getStatus(url, data);
			}

			//System.out.println("Brother "+this.getName()+"'s response to the "+operation+" is: "+output);
			logger.info("Brother "+this.getName()+"'s response to the "+operation+" is: "+output);


		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
}
