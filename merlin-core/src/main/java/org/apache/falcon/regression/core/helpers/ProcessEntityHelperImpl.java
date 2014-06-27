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

import com.jcraft.jsch.JSchException;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.xml.sax.InputSource;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;

public class ProcessEntityHelperImpl extends IEntityManagerHelper {

    private static Logger logger = Logger.getLogger(ProcessEntityHelperImpl.class);

    public ProcessEntityHelperImpl(String envFileName, String prefix) {
        super(envFileName, prefix);
    }

    public String getEntityType() {
        return "process";
    }

    public ServiceResponse delete(String url, String data, String user)
        throws IOException, URISyntaxException, JAXBException, AuthenticationException {
        return Util.sendRequest(createUrl(url, getEntityType(), readEntityName(data) + colo),
            "delete", user);
    }

    public ServiceResponse getEntityDefinition(String url, String data, String user)
        throws IOException, URISyntaxException, JAXBException, AuthenticationException {
        return Util.sendRequest(createUrl(url, getEntityType(), readEntityName(data)),
            "get", user);
    }

    public ServiceResponse getStatus(String url, String data, String user)
        throws IOException, URISyntaxException, JAXBException, AuthenticationException {
        return Util.sendRequest(createUrl(url, getEntityType(), readEntityName(data) + colo),
            "get", user);
    }

    public ServiceResponse submitAndSchedule(String url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        logger.info("Submitting process: " + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(url, getEntityType()),
            "post", data, user);
    }

    public ServiceResponse suspend(String url, String data, String user)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(createUrl(url, getEntityType(), Util.readEntityName(data) + colo),
            "post", user);
    }

    public ServiceResponse resume(String url, String data, String user)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(createUrl(url, getEntityType(), Util.readEntityName(data) + colo),
            "post", user);
    }

    public void validateResponse(String response, APIResult.Status expectedResponse,
                                 String filename) throws JAXBException, IOException {
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
    }

    public String readEntityName(String data) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        Process processElement = (Process) u.unmarshal((new StringReader(data)));
        return processElement.getName();
    }

    @Override
    public ProcessInstancesResult getRunningInstance(
        URLS processRunningInstance, String name, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RUNNING.getValue(), getEntityType(), name,
            "");
        return (ProcessInstancesResult) InstanceUtil
            .createAndsendRequestProcessInstance(url, null, allColo, user);
    }

    @Override
    public ProcessInstancesResult getProcessInstanceStatus(String EntityName, String params,
                                                           String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_STATUS.getValue(), getEntityType(),
            EntityName + "");
        return (ProcessInstancesResult) InstanceUtil
            .createAndsendRequestProcessInstance(url, params, allColo, user);
    }

    @Override
    public ProcessInstancesResult getProcessInstanceSuspend(
        String EntityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_SUSPEND.getValue(), getEntityType(),
            EntityName, "");
        return (ProcessInstancesResult) InstanceUtil
            .createAndsendRequestProcessInstance(url, params, allColo, user);
    }

    public ProcessInstancesResult getProcessInstanceResume(String EntityName, String params,
                                                           String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RESUME.getValue(),
            getEntityType(), EntityName, "");
        return (ProcessInstancesResult) InstanceUtil
            .createAndsendRequestProcessInstance(url, params, allColo, user);
    }

    @Override
    public InstancesSummaryResult getInstanceSummary(String entityName,
                                                     String params
    ) throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_SUMMARY.getValue(), getEntityType(),
            entityName, "");
        return (InstancesSummaryResult) InstanceUtil
            .createAndsendRequestProcessInstance(url, params, allColo, null);
    }

    public ProcessInstancesResult getProcessInstanceKill(String EntityName, String params,
                                                         String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_KILL.getValue(), getEntityType(),
            EntityName, "");
        return (ProcessInstancesResult) InstanceUtil
            .createAndsendRequestProcessInstance(url, params, allColo, user);

    }

    public ProcessInstancesResult getProcessInstanceRerun(String EntityName, String params,
                                                          String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RERUN.getValue(), getEntityType(),
            EntityName, "");
        return (ProcessInstancesResult) InstanceUtil
            .createAndsendRequestProcessInstance(url, params, allColo, user);
    }

    public String list() {
        return Util.executeCommandGetOutput(
            BASE_COMMAND + " entity -list -url " + this.hostname + " -type process");
    }

    @Override
    public String getDependencies(String entityName) {
        return Util.executeCommandGetOutput(
            BASE_COMMAND + " entity -dependency -url " + this.hostname +
                " -type process -name " + entityName);
    }

    @Override
    public ServiceResponse submitAndSchedule(URLS url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return submitAndSchedule(this.hostname + url.getValue(), data, user);
    }

    @Override
    public ServiceResponse resume(URLS url, String data, String user)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return resume(this.hostname + url.getValue(), data, user);
    }

    @Override
    public ServiceResponse getStatus(URLS url, String data, String user)
        throws IOException, URISyntaxException, JAXBException, AuthenticationException {
        return getStatus(this.hostname + url.getValue(), data, user);
    }

    @Override
    public ServiceResponse schedule(URLS scheduleUrl, String processData, String user)
        throws IOException, URISyntaxException, JAXBException, AuthenticationException {
        return schedule(this.hostname + scheduleUrl.getValue(), processData, user);
    }

    @Override
    public ServiceResponse delete(URLS deleteUrl, String data, String user)
        throws IOException, URISyntaxException, JAXBException, AuthenticationException {
        return delete(this.hostname + deleteUrl.getValue(), data, user);
    }

    @Override
    public ServiceResponse suspend(URLS suspendUrl, String data, String user)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return suspend(this.hostname + suspendUrl.getValue(), data, user);
    }

    @Override
    public List<String> getArchiveInfo() throws IOException, JSchException {
        return Util.getArchiveStoreInfo(this);
    }

    @Override
    public List<String> getStoreInfo() throws IOException, JSchException {
        return Util.getProcessStoreInfo(this);
    }

    @Override
    public ServiceResponse getEntityDefinition(URLS getUrl, String data, String user)
        throws IOException, URISyntaxException, JAXBException, AuthenticationException {
        return getEntityDefinition(this.hostname + getUrl.getValue(), data, user);
    }

    @Override
    public ServiceResponse update(String oldEntity, String newEntity, String user)
        throws IOException, JAXBException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.PROCESS_UPDATE.getValue(),
            Util.readEntityName(oldEntity));
        return Util.sendRequest(url + colo, "post", newEntity, user);
    }

    @Override
    public ServiceResponse update(String oldEntity, String newEntity, String updateTime,
                                  String user)
        throws IOException, JAXBException, URISyntaxException, AuthenticationException {
        return updateRequestHelper(oldEntity, newEntity,
            updateTime, URLS.PROCESS_UPDATE.getValue(), user);
    }

    @Override
    public String toString(Object object) throws JAXBException {
        Process processObject = (Process) object;
        JAXBContext context = JAXBContext.newInstance(Process.class);
        Marshaller um = context.createMarshaller();
        StringWriter writer = new StringWriter();
        um.marshal(processObject, writer);
        return writer.toString();
    }
}


