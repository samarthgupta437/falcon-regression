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

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;

public class ProcessMerlin extends Process {
    public ProcessMerlin(String processData) {
        this((Process) fromString(EntityType.PROCESS, processData));
    }

    public ProcessMerlin(final Process process) {
        try {
            PropertyUtils.copyProperties(this, process);
        } catch (IllegalAccessException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (NoSuchMethodException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    public final void setProperty(String name, String value) {
        Property p = new Property();
        p.setName(name);
        p.setValue(value);
        if (null == getProperties() || null == getProperties()
            .getProperties() || getProperties().getProperties().size()
            <= 0) {
            Properties props = new Properties();
            props.getProperties().add(p);
            setProperties(props);
        } else {
            getProperties().getProperties().add(p);
        }
    }

    @Override
    public String toString() {
        try {
            StringWriter sw = new StringWriter();
            EntityType.PROCESS.getMarshaller().marshal(this, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }
}


