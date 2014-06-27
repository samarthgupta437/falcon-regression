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
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;

public class ClusterEntityHelperImpl extends IEntityManagerHelper {


    private static Logger logger = Logger.getLogger(ClusterEntityHelperImpl.class);

    public ClusterEntityHelperImpl(String envFileName, String prefix) {
        super(envFileName, prefix);
    }

    public String getEntityType() {
        return "cluster";
    }

    public String getEntityName(String entity) throws JAXBException {
        return Util.readClusterName(entity);
    }

    public ServiceResponse getStatus(String url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse getStatus(URLS url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse resume(String url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse schedule(String url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse submitAndSchedule(String url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse submitAndSchedule(URLS url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse suspend(String url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ServiceResponse resume(URLS url, String data, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ProcessInstancesResult getRunningInstance(
        URLS processRunningInstance, String name, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ProcessInstancesResult getProcessInstanceStatus(
        String readEntityName, String params, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    public ProcessInstancesResult getProcessInstanceSuspend(
        String readEntityName, String params, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> getArchiveInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/archive/CLUSTER");
    }

    @Override
    public List<String> getStoreInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/CLUSTER");
    }

    @Override
    public ServiceResponse update(String oldEntity, String newEntity, String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ServiceResponse update(String oldEntity,
                                  String newEntity,
                                  String updateTime, String user)
        throws IOException, JAXBException {
        return null;
    }

    @Override
    public String toString(Object object) throws JAXBException {
        Cluster processObject = (Cluster) object;

        JAXBContext context = JAXBContext.newInstance(Cluster.class);
        Marshaller um = context.createMarshaller();
        StringWriter writer = new StringWriter();
        um.marshal(processObject, writer);
        return writer.toString();
    }

    @Override
    public ProcessInstancesResult getProcessInstanceKill(String readEntityName,
                                                         String string, String user) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProcessInstancesResult getProcessInstanceRerun(
        String readEntityName, String string, String user) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProcessInstancesResult getProcessInstanceResume(
        String readEntityName, String string, String user) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InstancesSummaryResult getInstanceSummary(String readEntityName,
                                                     String string
    ) throws
        IOException, URISyntaxException {
        logger.info("Not Valid for Cluster Entity");
        return null;
    }

}
