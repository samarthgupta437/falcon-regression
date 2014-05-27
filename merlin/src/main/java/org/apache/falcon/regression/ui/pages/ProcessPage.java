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

import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.util.List;

public class ProcessPage extends EntityPage<Process> {

    private final static String INSTANCE_STATUS_TEMPLATE = "//div[@id='panel-instance']//span[contains(..,'%s')]";
    private final static String LINEAGE_LINK_TEMPLATE = "//a[@class='lineage-href' and @data-instance-name='%s']";

    public ProcessPage(WebDriver driver, PrismHelper helper, String entityName)  {
        super(driver, helper, ENTITY_TYPE.PROCESS, Process.class, entityName);
    }

    public String getInstanceStatus(String instanceDate) {
        List<WebElement> status = driver.findElements(By.xpath(String.format(INSTANCE_STATUS_TEMPLATE, instanceDate)));
        if (status.isEmpty()) {
            return null;
        } else {
            return status.get(0).getAttribute("class").replace("instance-icons instance-link-", "");
        }
    }

    public boolean isLineageLinkPresent(String instanceDate) {
        List<WebElement> lineage = driver.findElements(By.xpath(String.format(LINEAGE_LINK_TEMPLATE, instanceDate)));
        return !lineage.isEmpty();
    }
}
