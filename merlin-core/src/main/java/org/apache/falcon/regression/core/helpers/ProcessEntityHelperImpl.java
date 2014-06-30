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
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

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

    public String getEntityName(String entity) throws JAXBException {
        return Util.getProcessName(entity);
    }

    public String readEntityName(String data) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        Process processElement = (Process) u.unmarshal((new StringReader(data)));
        return processElement.getName();
    }

    @Override
    public List<String> getArchiveInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/archive/PROCESS");
    }

    @Override
    public List<String> getStoreInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/PROCESS");
    }

}


