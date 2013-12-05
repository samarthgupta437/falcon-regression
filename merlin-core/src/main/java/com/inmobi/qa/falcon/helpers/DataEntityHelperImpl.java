/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.helpers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.inmobi.qa.falcon.generated.feed.Feed;
import com.inmobi.qa.falcon.interfaces.IEntityManagerHelper;
import com.inmobi.qa.falcon.response.ProcessInstancesResult;
import com.inmobi.qa.falcon.util.Util;
import org.testng.Assert;
import org.testng.log4testng.Logger;
import org.xml.sax.InputSource;

import com.inmobi.qa.falcon.response.APIResult;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.util.instanceUtil;

/**
 *
 * @author rishu.mehrotra
 */
public class DataEntityHelperImpl extends IEntityManagerHelper {

    Logger logger=Logger.getLogger(this.getClass());
    
    public DataEntityHelperImpl()
    {
    }
    
    public DataEntityHelperImpl(String envFileName) throws Exception
    {
        super(envFileName);
    }
    
    public ServiceResponse delete(String url, String data) throws Exception {
          
          //throw new UnsupportedOperationException("Not supported yet.");
            return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo);
//        url+="/process/"+readEntityName(data);
    }

    public ServiceResponse getEntityDefinition(String url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        return Util.sendRequest(url+"/feed/"+Util.readDatasetName(data));
    }
    public ServiceResponse updateFeed(String processName,String newProcess) throws Exception {

		String url = this.hostname+ Util.URLS.FEED_UPDATE.getValue() + "/"+ processName ;
		return Util.sendRequest(url+colo,newProcess);
	}
    
    public ServiceResponse getEntityDefinition(Util.URLS url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        return getEntityDefinition(this.hostname+url.getValue(), data);
    }

    public ServiceResponse getStatus(String url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        return Util.sendRequest(url+"/feed/"+Util.readDatasetName(data)+colo);
    }
    
    public ServiceResponse getStatus(Util.URLS url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        return getStatus(this.hostname+url.getValue(),data);
    }

    public ServiceResponse resume(String url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        return Util.sendRequest(url+"/feed/"+Util.readDatasetName(data)+colo);
    }

    public ServiceResponse schedule(String url, String data) throws Exception {
        url+="/feed/"+Util.readDatasetName(data)+colo;
	return Util.sendRequest(url);
    }

    public ServiceResponse submitAndSchedule(String url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        return Util.sendRequest(url+"/feed"+colo, data);
    }
    
    public ServiceResponse submitAndSchedule(Util.URLS url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        return submitAndSchedule(this.hostname+url.getValue(), data);
    }

    public ServiceResponse submitEntity(String url, String data) throws Exception {
        
        url+="/feed"+colo;
         
        return Util.sendRequest(url, data);
    }

    public ServiceResponse suspend(String url, String data) throws Exception {
        return Util.sendRequest(url+"/feed/"+Util.readDatasetName(data)+colo);
    }
    
    public ServiceResponse suspend(Util.URLS url, String data) throws Exception {
        return suspend(this.hostname+url.getValue(),data);
    }

    public ServiceResponse validateEntity(String url, String data) throws Exception {
        
        if(!(Thread.currentThread().getStackTrace()[1].getMethodName().contains("Wrong")))
        {
           url+="/feed"; 
        }
        
        return Util.sendRequest(url+colo, data);
    }

    public void validateResponse(String response, APIResult.Status expectedResponse,String filename) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        JAXBContext jc=JAXBContext.newInstance(APIResult.class); 

		Unmarshaller u=jc.createUnmarshaller();

		APIResult result=(APIResult)u.unmarshal(new InputSource(new StringReader(response)));

		Assert.assertEquals(expectedResponse,result.getStatus(),"Status message does not match with expected one!");

		if(expectedResponse.equals(APIResult.Status.FAILED))
		{
			//now to check for the correct error message!
			Assert.assertEquals(result.getMessage(),Util.getExpectedErrorMessage(filename),"Error message does not match in failure case!");
		}
		else
		{
			Assert.assertEquals(result.getMessage(),"Validate successful","validation success message does not match in valid case!");
		}
    }

	@Override
	public ServiceResponse submitEntity(Util.URLS url, String data) throws Exception {
		return submitEntity(this.hostname+url.getValue(),data);
	}

	@Override
	public ServiceResponse schedule(Util.URLS scheduleUrl, String processData)
			throws Exception {
            return schedule(this.hostname+scheduleUrl.getValue(),processData);
	}

	@Override
	public ServiceResponse delete(Util.URLS deleteUrl, String data) throws Exception {
		
            //String url=deleteUrl.getValue()+"/feed/"+Util.readDatasetName(data);
            //return Util.sendRequest(url);
            
            return delete(this.hostname+deleteUrl.getValue(), data);
            
	}

	@Override
	public ServiceResponse resume(Util.URLS url, String data) throws Exception {
		// TODO Auto-generated method stub
		return resume(this.hostname+url.getValue(), data);
	}

	
    @Override
	public ProcessInstancesResult getRunningInstance(
			String processRuningInstance, String name) throws Exception {

		String url = this.hostname+processRuningInstance + "/" + name+allColo;

		return instanceUtil.sendRequestProcessInstance(url);
	}
        
	@Override
	public ProcessInstancesResult getRunningInstance(
			Util.URLS processRuningInstance, String name) throws Exception {

		String url = this.hostname+processRuningInstance.getValue() + "/feed/" + name+allColo;

		return instanceUtil.sendRequestProcessInstance(url);
	}
	
	@Override
	public ProcessInstancesResult getProcessInstanceStatus(
			String EntityName, String params) throws Exception {

		String url = this.hostname+ Util.URLS.INSTANCE_STATUS.getValue() + "/"+"feed/" + EntityName + "/";
	
		return instanceUtil.createAndsendRequestProcessInstance(url,params,allColo);
	}
	
	
	@Override
	public ProcessInstancesResult getProcessInstanceSuspend(
			String EntityName, String params) throws Exception {
		String url = this.hostname+ Util.URLS.INSTANCE_SUSPEND.getValue() + "/"+"feed/"  + EntityName + "/";
		
		return instanceUtil.createAndsendRequestProcessInstance(url,params,allColo);


	}
        
	
	public ProcessInstancesResult getProcessInstanceResume(String EntityName, String params) throws Exception {
		String url = this.hostname+ Util.URLS.INSTANCE_RESUME.getValue() + "/" +"feed/" + EntityName + "/";
		return instanceUtil.createAndsendRequestProcessInstance(url,params,allColo);

	}
	
	public ProcessInstancesResult getProcessInstanceKill(String EntityName, String params) throws Exception {
		String url = this.hostname+ Util.URLS.INSTANCE_KILL.getValue() + "/"+"feed/"  + EntityName + "/";
		return instanceUtil.createAndsendRequestProcessInstance(url,params,allColo);

	}
	
	public ProcessInstancesResult getProcessInstanceRerun(String EntityName, String params) throws Exception {
		String url = this.hostname+ Util.URLS.INSTANCE_RERUN.getValue() + "/"  +"feed/"+ EntityName + "/";
		return instanceUtil.createAndsendRequestProcessInstance(url,params,allColo);

	}
        
         
        public String writeEntityToFile(String entity) throws Exception
        {
            File file =new File("/tmp/"+Util.readDatasetName(entity)+".xml");
            BufferedWriter bf=new BufferedWriter(new FileWriter(file));
            bf.write(entity);
            bf.close();
            return "/tmp/"+Util.readDatasetName(entity)+".xml";
        }

    @Override
    public String submitEntityViaCLI(String filePath) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -submit -url "+this.hostname+" -type feed -file "+filePath);
    }

    @Override
    public String validateEntityViaCLI(String entityName) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -validate -url "+this.hostname+" -type feed -name "+entityName);
    }

    @Override
    public String submitAndScheduleViaCLI(String filePath) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -submitAndSchedule -url "+this.hostname+" -type feed -file "+filePath);
    }

    @Override
    public String scheduleViaCLI(String entityName) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -schedule -url "+this.hostname+" -type feed -name "+entityName);
    }

    @Override
    public String resumeViaCLI(String entityName) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -resume -url "+this.hostname+" -type feed -name "+entityName);
    }

    @Override
    public String getStatusViaCLI(String entityName) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -status -url "+this.hostname+" -type feed -name "+entityName);
    }

    @Override
    public String getEntityDefinitionViaCLI(String entityName) throws Exception {
       
        return Util.executeCommand(BASE_COMMAND+ " entity -definition -url "+this.hostname+" -type feed -name "+entityName);
    }

    @Override
    public String deleteViaCLI(String entityName) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -delete -url "+this.hostname+" -type feed -name "+entityName);
    }

    @Override
    public String suspendViaCLI(String entityName) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -suspend -url "+this.hostname+" -type feed -name "+entityName);
    }

    public String updateViaCLI(String processName,String newProcessFilePath) throws Exception
    {
        return null;
    }
    
    public String list() throws Exception
    {
        return Util.executeCommand(BASE_COMMAND+ " entity -list -url "+this.hostname+" -type feed");
    }

    @Override
    public String getDependencies(String entityName) throws Exception {
        
        return Util.executeCommand(BASE_COMMAND+ " entity -dependency -url "+this.hostname+" -type feed -name "+entityName);
    }

   
    @Override
    public List<String> getArchiveInfo() throws Exception {
        
        return Util.getDataSetArchiveInfo(this);
    }

    @Override
    public List<String> getStoreInfo() throws Exception {
        
        return Util.getDataSetStoreInfo(this);
    }

    @Override
    public ServiceResponse update(String oldEntity, String newEntity) throws Exception {
        
        String url = this.hostname+ Util.URLS.FEED_UPDATE.getValue() + "/"+ Util.readDatasetName(oldEntity) ;
		return Util.sendRequest(url+colo,newEntity);
    }
    
    
    public ServiceResponse update(String newEntity) throws Exception {
        
        String url = this.hostname+ Util.URLS.FEED_UPDATE.getValue() + "/"+ Util.readDatasetName(newEntity) ;
		return Util.sendRequest(url+colo,newEntity);
    }
    
   @Override
    public String toString(Object object) throws Exception
    {
        Feed processObject=(Feed)object;
        
        JAXBContext context=JAXBContext.newInstance(Feed.class);
        Marshaller um=context.createMarshaller();
        StringWriter writer=new StringWriter();
        um.marshal(processObject,writer);
        return writer.toString();
    }
   
       @Override
        public ProcessInstancesResult getInstanceRerun(String EntityName, String params) throws Exception {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String getProcessInstanceStatusViaCli(String EntityName,
			String start, String end, String colos) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	
}
