/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.interfaces;

import java.util.List;
import java.util.Properties;

import com.inmobi.qa.falcon.response.APIResult;
import com.inmobi.qa.falcon.response.ProcessInstancesResult;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;

/**
 *
 * @author rishu.mehrotra
 */
public abstract class IEntityManagerHelper {
    
    
   // final String CLIENT_LOCATION="src/test/resources/IvoryClient/ivory-client-3.jar";
    //final String CLIENT_LOCATION="src/test/resources/IvoryClient/ivory-client.jar";
    protected String CLIENT_LOCATION="src/test/resources/IvoryClient/IvoryCLI.jar";
    protected String BASE_COMMAND="java -jar "+CLIENT_LOCATION;

    public String getBASE_COMMAND() {
        return BASE_COMMAND;
    }

    public String getCLIENT_LOCATION() {
        return CLIENT_LOCATION;
    }
    
    public String getActiveMQ() {
        return activeMQ;
    }

    public String getHadoopGetCommand() {
        return hadoopGetCommand;
    }

    public String getHadoopLocation() {
        return hadoopLocation;
    }

    public String getHadoopURL() {
        return hadoopURL;
    }

    public String getHostname() {
        return hostname;
    }

    public String getOozieLocation() {
        return oozieLocation;
    }

    public String getOozieURL() {
        return oozieURL;
    }

    public String getPassword() {
        return password;
    }

    public String getStoreLocation() {
        return storeLocation;
    }

    public String getUsername() {
        return username;
    }
    
    //basic properties
    protected String qaHost;

    public String getQaHost() {
        return qaHost;
    }
    protected String hostname="";
    protected String username="";
    protected String password="";
    protected String hadoopLocation="";
    protected String hadoopURL="";
    protected String oozieURL="";
    protected String oozieLocation="";
    protected String activeMQ="";
    protected String storeLocation="";
    protected String hadoopGetCommand="";
    protected String envFileName;
    protected String colo;
    protected String allColo;
    protected String serviceStartCmd;
    protected String serviceStopCmd;
    protected String serviceRestartCmd;
    protected String serviceStatusCmd;

  public String getIdentityFile() {
    return identityFile;
  }

  protected String identityFile;

  public String getServiceStatusMsg() {
    return serviceStatusMsg;
  }

  public String getServiceStatusCmd() {
    return serviceStatusCmd;
  }

  protected String serviceStatusMsg;

  public String getServiceUser() {
    return serviceUser;
  }

  public String getServiceRestartCmd() {
    return serviceRestartCmd;
  }

  public String getServiceStopCmd() {
    return serviceStopCmd;
  }

  public String getServiceStartCmd() {
    return serviceStartCmd;
  }

  protected String serviceUser;
 
    public String getEnvFileName() {
		return envFileName;
	}

	public IEntityManagerHelper()
    {
        
    }
    
    public IEntityManagerHelper(String envFileName) throws Exception
    {
       Properties prop = Util.getPropertiesObj(envFileName);
       this.qaHost=prop.getProperty("qa_host");
       this.hostname=prop.getProperty("ivory_hostname");
       this.username=prop.getProperty("username", System.getProperty("user.name"));
       this.password=prop.getProperty("password", "");
       this.hadoopLocation=prop.getProperty("hadoop_location");
       this.hadoopURL=prop.getProperty("hadoop_url");
       this.oozieURL=prop.getProperty("oozie_url");
       this.oozieLocation=prop.getProperty("oozie_location");
       this.activeMQ=prop.getProperty("activemq_url");
       this.storeLocation=prop.getProperty("storeLocation");
       this.hadoopGetCommand=hadoopLocation+"  fs -cat hdfs://"+hadoopURL+"/projects/ivory/staging/ivory/workflows/process";
       this.envFileName=envFileName;
       this.allColo = "?colo=" + prop.getProperty("colo", "*");
       this.colo = (!prop.getProperty("colo", "").isEmpty()) ? "?colo=" + prop
         .getProperty("colo") : "";
       this.serviceStartCmd = prop.getProperty("service_start_cmd","/etc/init.d/tomcat6 start");
       this.serviceStopCmd = prop.getProperty("service_stop_cmd",
         "/etc/init.d/tomcat6 stop");
       this.serviceRestartCmd = prop.getProperty("service_restart_cmd",
         "/etc/init.d/tomcat6 restart");
       this.serviceUser = prop.getProperty("service_user", null);
       this.serviceStatusMsg = prop.getProperty("service_status_msg",
         "Tomcat servlet engine is running with pid");
       this.serviceStatusCmd = prop.getProperty("service_status_cmd", "/etc/init.d/tomcat6 status");
       this.identityFile = prop.getProperty("identityFile",
         System.getProperty("user.home") +"/.ssh/id_rsa");
    }
    
    public abstract ServiceResponse submitEntity(String url, String data) throws Exception;

    public abstract ServiceResponse submitEntity(Util.URLS url, String data) throws Exception;

    public abstract ServiceResponse validateEntity(String url, String data) throws Exception;

    public abstract ServiceResponse schedule(String url, String data) throws Exception;

    public abstract ServiceResponse submitAndSchedule(String url, String data) throws Exception;

    public abstract ServiceResponse submitAndSchedule(URLS url, String data) throws Exception;

    public abstract ServiceResponse delete(String url, String data) throws Exception;

    public abstract ServiceResponse suspend(String url, String data) throws Exception;

    public abstract ServiceResponse resume(String url, String data) throws Exception;

    public abstract ServiceResponse resume(URLS url, String data) throws Exception;

    public abstract ServiceResponse getStatus(String url, String data) throws Exception;

    public abstract ServiceResponse getStatus(URLS url, String data) throws Exception;

    public abstract ServiceResponse getEntityDefinition(String url, String data) throws Exception;
    
    public abstract ServiceResponse getEntityDefinition(URLS url, String data) throws Exception;

    
    public abstract void validateResponse(String response,APIResult.Status expectedResponse,String filename) throws Exception;

    public abstract ServiceResponse schedule(URLS scheduleUrl, String processData) throws Exception;

    public abstract ServiceResponse delete(URLS deleteUrl, String data) throws Exception;

    public abstract ServiceResponse suspend(URLS suspendUrl, String data) throws Exception;

    public abstract ProcessInstancesResult getRunningInstance(URLS processRuningInstance, String name) throws Exception;
    
        public abstract ProcessInstancesResult getRunningInstance(String processRuningInstance, String name) throws Exception;

    public abstract ProcessInstancesResult getProcessInstanceStatus(
            String readEntityName, String params) throws Exception;

    public abstract ProcessInstancesResult getProcessInstanceSuspend(
            String readEntityName, String params) throws Exception;
    
    public abstract String writeEntityToFile(String entity) throws Exception;
    
    public abstract String submitEntityViaCLI(String filePath) throws Exception;
    
    public abstract String validateEntityViaCLI(String filePath) throws Exception;
    
    public abstract String submitAndScheduleViaCLI(String filePath) throws Exception;
    
    public abstract String scheduleViaCLI(String filePath) throws Exception;

    public abstract String resumeViaCLI(String filePath) throws Exception;

    public abstract String getStatusViaCLI(String filePath) throws Exception;

    public abstract String getEntityDefinitionViaCLI(String filePath) throws Exception;

    public abstract String deleteViaCLI(String filePath) throws Exception;

    public abstract String suspendViaCLI(String filePath) throws Exception;
    public abstract String updateViaCLI(String processName,String newProcessFilePath) throws Exception;
    
    public abstract String list() throws Exception;
    
    public abstract String getDependencies(String entityName) throws Exception;
    
    public abstract List<String> getArchiveInfo() throws Exception;
    
    public abstract List<String> getStoreInfo() throws Exception;
    
    public abstract ServiceResponse update(String oldEntity,String newEntity) throws Exception;
    
    public abstract String toString(Object object) throws Exception;
    
    public abstract ProcessInstancesResult getInstanceRerun(String EntityName, String params) throws Exception;

	 public abstract  ProcessInstancesResult getProcessInstanceKill(String readEntityName,
			String string) throws Exception ;

	public abstract ProcessInstancesResult getProcessInstanceRerun(String readEntityName, String string) throws Exception ;

	public abstract ProcessInstancesResult getProcessInstanceResume(String readEntityName, String string) throws Exception ;
	
        public String getColo() {
        return colo;
    }

	public abstract String getProcessInstanceStatusViaCli(String EntityName,
			String start, String end, String colos) throws Exception;
}
