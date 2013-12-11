/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.falcon.regression.core.helpers;

import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.testng.Assert;
import org.xml.sax.InputSource;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

public class ProcessEntityHelperImpl extends IEntityManagerHelper {

    public ProcessEntityHelperImpl() {

    }

    public ProcessEntityHelperImpl(String envFileName) throws Exception {
        super(envFileName);
    }

    public ServiceResponse delete(String url, String data) throws Exception {

        //        if(!(Thread.currentThread().getStackTrace()[3].getMethodName().contains("Wrong")))
        //        {
        //           url+="/process/"+readEntityName(data);
        //        }

        url += "/process/" + readEntityName(data) + colo;

        //System.out.println("printing the response: " + Util.sendRequest(url, data));

        return Util.sendRequest(url);
    }

//	public ServiceResponse delete(URLS url, String data) throws Exception {
//		// TODO Auto-generated method stub
//		return delete(url.getValue(), data);
//	}

    public ServiceResponse getEntityDefinition(String url, String data) throws Exception {

        //        if(!(Thread.currentThread().getStackTrace()[3].getMethodName().contains("Wrong")))
        //        {
        //           url+="/"+"process/"+readEntityName(data);
        //        }

        url += "/process/" + readEntityName(data);

        return Util.sendRequest(url);

    }

    public ServiceResponse getStatus(String url, String data) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        url += "/process/" + readEntityName(data) + colo;
        return Util.sendRequest(url);
    }

//	public ServiceResponse getStatus(URLS url, String data) throws Exception {
//		//throw new UnsupportedOperationException("Not supported yet.");
//		return getStatus(url.getValue(), data);
//	}

    public ServiceResponse schedule(String url, String data) throws Exception {

        url += "/process/" + readEntityName(data) + colo;
        return Util.sendRequest(url);
    }

//	public ServiceResponse schedule(Util.URLS url, String data) throws Exception {
//		return schedule(url.getValue(), data);
//	}

    public ServiceResponse submitAndSchedule(String url, String data) throws Exception {

        url += "/process";
        return Util.sendRequest(url, data);

    }

//	public ServiceResponse submitAndSchedule(URLS url, String data) throws Exception {
//
//		ServiceResponse response = submitAndSchedule(url.getValue(), data);
//		return response;
//	}

    public ServiceResponse submitEntity(String url, String data) throws Exception {

        //    	 if(!(Thread.currentThread().getStackTrace()[3].getMethodName().contains("Wrong")))
        //         {
        //            url+="/process";
        //         }

        url += "/process";

        return Util.sendRequest(url, data);


    }

//	public ServiceResponse submitEntity(Util.URLS url, String data) throws Exception {
//
//		return submitEntity(url.getValue(), data);
//	}

    public ServiceResponse suspend(String url, String data) throws Exception {

        return Util.sendRequest(url + "/process/" + Util.readEntityName(data) + colo);
    }

//	public ServiceResponse suspend(URLS url, String data) throws Exception {
//		return suspend(url.getValue(), data);
//	}

    public ServiceResponse resume(String url, String data) throws Exception {
        return Util.sendRequest(url + "/process/" + Util.readEntityName(data) + colo);
    }

//	public ServiceResponse resume(Util.URLS url, String data) throws Exception {
//		return resume(url.getValue(), data);
//	}

    /*public ServiceResponse validateEntity(String url, String data) throws Exception {

        //System.out.println(Thread.currentThread().getStackTrace()[2].getMethodName());
        if (!(Thread.currentThread().getStackTrace()[3].getMethodName().contains("Wrong"))) {
            url += "/process";
        }


        return Util.sendRequest(url, data);
    }*/

    public void validateResponse(String response, APIResult.Status expectedResponse,
                                 String filename) throws Exception {

        JAXBContext jc = JAXBContext.newInstance(APIResult.class);

        Unmarshaller u = jc.createUnmarshaller();

        APIResult result = (APIResult) u.unmarshal(new InputSource(new StringReader(response)));

        Assert.assertEquals(expectedResponse, result.getStatus(),
                "Status message does not match with expected one!");

        if (expectedResponse.equals(APIResult.Status.FAILED)) {
            //now to check for the correct error message!
            Assert.assertEquals(result.getMessage(), Util.getExpectedErrorMessage(filename),
                    "Error message does not match in failure case!");
        } else {
            Assert.assertEquals(result.getMessage(), "Validate successful",
                    "validation success message does not match in valid case!");
        }

        //System.out.println(result.getMessage());


        //       SchemaFactory factory=SchemaFactory.newInstance("http://www.w3
        // .org/2001/XMLSchema");
        //
        //       File schemaLocation = new File("src/test/resources/xsd/process.xsd");
        //       Schema schema = factory.newSchema(schemaLocation);
        //
        //       Validator validator = schema.newValidator();
        //
        //       SAXSource source=new SAXSource(new InputSource(new StringReader(response)));
        //
        //       validator.validate(source);


    }

    public String readEntityName(String data) throws Exception {


        JAXBContext jc = JAXBContext.newInstance(Process.class);

        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(data)));

        return processElement.getName();

    }


    /*@Override
    public ProcessInstancesResult getRunningInstance(
            String processRuningInstance, String name) throws Exception {

        String url = this.hostname + processRuningInstance + "/" + name + allColo;

        return InstanceUtil.sendRequestProcessInstance(url);
    }*/

    @Override
    public ProcessInstancesResult getRunningInstance(
            URLS processRuningInstance, String name) throws Exception {

        String url =
                this.hostname + URLS.INSTANCE_RUNNING.getValue() + "/" + "process/" + name + "/";

        return InstanceUtil.createAndsendRequestProcessInstance(url, null, allColo);
    }

    @Override
    public ProcessInstancesResult getProcessInstanceStatus(String EntityName, String params)
    throws Exception {


        String url =
                this.hostname + URLS.INSTANCE_STATUS.getValue() + "/" + "process/" + EntityName +
                        "/";

        return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);
    }


    /*@Override
    public String getProcessInstanceStatusViaCli(
            String EntityName, String start, String end, String colos)
    throws Exception {
        String command = "";
        if (end != null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    EntityName +
                    " -type process " + " -start " + start + " -end " + end;
        else
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    EntityName +
                    " -type process " + " -start " + start;


        return Util.executeCommand(command);
    }*/


    @Override
    public ProcessInstancesResult getProcessInstanceSuspend(
            String EntityName, String params) throws Exception {
        String url =
                this.hostname + URLS.INSTANCE_SUSPEND.getValue() + "/" + "process/" + EntityName +
                        "/";

        return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);


    }

    public ProcessInstancesResult getProcessInstanceResume(String EntityName, String params)
    throws Exception {
        String url =
                this.hostname + URLS.INSTANCE_RESUME.getValue() + "/" + "process/" + EntityName +
                        "/";
        return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);

    }

    public ProcessInstancesResult getProcessInstanceKill(String EntityName, String params)
    throws Exception {
        String url =
                this.hostname + URLS.INSTANCE_KILL.getValue() + "/" + "process/" + EntityName + "/";
        return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);

    }

    /*public ServiceResponse updateProcess(String processName, String newProcess) throws Exception {

        String url = this.hostname + URLS.PROCESS_UPDATE.getValue() + "/" + processName;
        return Util.sendRequest(url + colo, newProcess);
    }

    public String updateViaCLI(String processName, String newProcessFilePath) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -update -url " + this.hostname + " -type process -name " +
                        processName +
                        " -file " + newProcessFilePath);
    }

    @Override
    public String validateEntityViaCLI(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -validate -url " + this.hostname + " -type process -name " +
                        entityName);
    }

    @Override
    public String submitAndScheduleViaCLI(String filePath) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -submitAndSchedule -url " + this.hostname +
                        " -type process -file " + filePath);
    }

    @Override
    public String scheduleViaCLI(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -schedule -url " + this.hostname + " -type process -name " +
                        entityName);
    }

    @Override
    public String resumeViaCLI(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -resume -url " + this.hostname + " -type process -name " +
                        entityName);
    }

    @Override
    public String getStatusViaCLI(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -status -url " + this.hostname + " -type process -name " +
                        entityName);
    }

    @Override
    public String getEntityDefinitionViaCLI(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -definition -url " + this.hostname +
                        " -type process -name " + entityName);
    }

    @Override
    public String deleteViaCLI(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -delete -url " + this.hostname + " -type process -name " +
                        entityName);
    }

    @Override
    public String suspendViaCLI(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -suspend -url " + this.hostname + " -type process -name " +
                        entityName);
    }  */

    public ProcessInstancesResult getProcessInstanceRerun(String EntityName, String params)
    throws Exception {
        String url =
                this.hostname + URLS.INSTANCE_RERUN.getValue() + "/" + "process/" + EntityName +
                        "/";
        return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);

    }

    /*@Override
    public ProcessInstancesResult getInstanceRerun(String EntityName, String params)
    throws Exception {
        String url =
                this.hostname + URLS.INSTANCE_RERUN.getValue() + "/" + "process/" + EntityName +
                        "/";
        return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);

    }


    @Override
    public String submitEntityViaCLI(String filePath) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -submit -url " + this.hostname + " -type process -file " +
                        filePath);
    }

    public String writeEntityToFile(String entity) throws Exception {
        File file = new File("/tmp/" + Util.readEntityName(entity) + ".xml");
        BufferedWriter bf = new BufferedWriter(new FileWriter(file));
        bf.write(entity);
        bf.close();
        return "/tmp/" + Util.readEntityName(entity) + ".xml";
    } */

    /*public String getProcessInstanceStatusCLI(
            String processName, String start, String end) throws Exception {
        String command = "";
        if (end != null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end;
        else
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start;

        return Util.executeCommand(command);

    }*/

    /*public String CLIHelp() throws Exception {
        return Util.executeCommand(BASE_COMMAND + " help");
    }*/

    /*public String getProcessInstanceRunningCLI(String processName) throws Exception {
        String command =
                BASE_COMMAND + " instance -running -url " + this.hostname + " -processName " +
                        processName;
        return Util.executeCommand(command);
    }*/

    /*public String getProcessInstanceSuspendCLI(String processName,
                                               String start, String end) throws Exception {
        String command = "";
        if (end != null)
            command = BASE_COMMAND + " instance -suspend -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end;
        else
            command = BASE_COMMAND + " instance -suspend -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start;

        return Util.executeCommand(command);
    }*/

    /*public String processInstanceRerunCLI(String processName,
                                          String start, String end) throws Exception {
        String command = "";
        if (end != null)
            command = BASE_COMMAND + " instance -rerun -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end;
        else
            command = BASE_COMMAND + " instance -rerun -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start;

        return Util.executeCommand(command);
    }*/

    /*public String processInstanceResumeCLI(String processName,
                                           String start, String end) throws Exception {
        String command = "";
        if (end != null)
            command = BASE_COMMAND + " instance -resume -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end;
        else
            command = BASE_COMMAND + " instance -resume -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start;

        return Util.executeCommand(command);
    }*/

    /*public String processInstanceKillCLI(String processName, String start,
                                         String end) throws Exception {
        String command = "";
        if (end != null)
            command = BASE_COMMAND + " instance -kill -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end;
        else
            command = BASE_COMMAND + " instance -kill -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start;

        return Util.executeCommand(command);
    }*/

    /*public String processInstanceSuspendCLI(String processName,
                                            String start, String end) throws Exception {
        String command = "";
        if (end != null)
            command = BASE_COMMAND + " instance -suspend -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end;
        else
            command = BASE_COMMAND + " instance -suspend -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start;

        return Util.executeCommand(command);
    }*/

    public String list() throws Exception {
        return Util.executeCommand(
                BASE_COMMAND + " entity -list -url " + this.hostname + " -type process");
    }

    @Override
    public String getDependencies(String entityName) throws Exception {

        return Util.executeCommand(
                BASE_COMMAND + " entity -dependency -url " + this.hostname +
                        " -type process -name " + entityName);
    }

    /*public String getProcessInstanceStatusCLI(String processName,
                                              String start, String end, int runid, String type)
    throws Exception {
        String command = "";
        if (end != null && runid >= 0 && type != null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end + " -runid " + runid + " -type " + type;
        else if (end != null && runid >= 0 && type == null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end + " -runid " + runid;
        else if (end != null && runid < 0 && type == null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end;
        else if (end != null && runid > 0 && type != null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -end " + end + " -type " + type;
        else if (end == null && runid >= 0 && type != null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -runid " + runid + " -type " + type;
        else if (end == null && runid >= 0 && type == null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -runid " + runid;
        else if (end == null && runid < 0 && type == null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start;
        else if (end == null && runid > 0 && type != null)
            command = BASE_COMMAND + " instance -status -url " + this.hostname + " -processName " +
                    processName +
                    " -start " + start + " -type " + type;


        return Util.executeCommand(command);

    }*/

    @Override
    public ServiceResponse submitEntity(URLS url, String data) throws Exception {

        return submitEntity(this.hostname + url.getValue(), data);
    }

    @Override
    public ServiceResponse submitAndSchedule(URLS url, String data) throws Exception {
        return submitAndSchedule(this.hostname + url.getValue(), data);
    }

    @Override
    public ServiceResponse resume(URLS url, String data) throws Exception {
        return resume(this.hostname + url.getValue(), data);
    }

    @Override
    public ServiceResponse getStatus(URLS url, String data) throws Exception {
        return getStatus(this.hostname + url.getValue(), data);
    }

    @Override
    public ServiceResponse schedule(URLS scheduleUrl, String processData) throws Exception {
        return schedule(this.hostname + scheduleUrl.getValue(), processData);
    }

    @Override
    public ServiceResponse delete(URLS deleteUrl, String data) throws Exception {

        return delete(this.hostname + deleteUrl.getValue(), data);
    }

    @Override
    public ServiceResponse suspend(URLS suspendUrl, String data) throws Exception {

        return suspend(this.hostname + suspendUrl.getValue(), data);
    }

    @Override
    public List<String> getArchiveInfo() throws Exception {

        return Util.getArchiveStoreInfo(this);
    }

    @Override
    public List<String> getStoreInfo() throws Exception {

        return Util.getProcessStoreInfo(this);
    }

    @Override
    public ServiceResponse getEntityDefinition(URLS getUrl, String data)
    throws Exception {
        return getEntityDefinition(this.hostname + getUrl.getValue(), data);
    }

    @Override
    public ServiceResponse update(String oldEntity, String newEntity) throws Exception {

        String url = this.hostname + URLS.PROCESS_UPDATE.getValue() + "/" +
                Util.readEntityName(oldEntity);
        return Util.sendRequest(url + colo, newEntity);
    }

    @Override
    public String toString(Object object) throws Exception {
        Process processObject = (Process) object;

        JAXBContext context = JAXBContext.newInstance(Process.class);
        Marshaller um = context.createMarshaller();
        StringWriter writer = new StringWriter();
        um.marshal(processObject, writer);
        return writer.toString();
    }
}


