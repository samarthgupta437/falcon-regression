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

package org.apache.falcon.regression.ui.pages;


import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;

public class EntityPage<T> extends Page {

    private Class<T> type;

    public EntityPage(WebDriver driver, PrismHelper helper, ENTITY_TYPE type, Class<T> entity, String entityName) {
        super(driver, helper);
        URL += String.format("/entity.html?type=%s&id=%s", type, entityName);
        this.type = entity;
        expectedElement = "//textarea[@id='entity-def-textarea' and contains(text(), 'xml')]";
        notFoundMsg = String.format(" %s '%s' not found!", type, entityName);
    }

    public T getEntity() throws JAXBException, IOException {
        String entity = driver.findElement(By.id("entity-def-textarea")).getText();
        JAXBContext jc = JAXBContext.newInstance(type);
        Unmarshaller u = jc.createUnmarshaller();
        return (T) u.unmarshal(new StringReader(entity));
    }

}