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

import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.openqa.selenium.By;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

import javax.annotation.Nullable;


public abstract class Page {
    protected final static int DEFAULT_TIMEOUT = 10;
    protected String URL;
    protected WebDriver driver;

    protected String expectedElement;
    protected String notFoundMsg;

    Page(WebDriver driver, PrismHelper helper) {
        this.driver = driver;
        URL = helper.getClusterHelper().getHostname();
    }
    
    public void navigateTo() {
        driver.get(URL);
        waitForElement(expectedElement, DEFAULT_TIMEOUT);
    }

    public void refresh() {
        driver.navigate().refresh();
    }

    public void waitForElement(final String xpath, final long timeoutSeconds) {

        try {
            new WebDriverWait(driver, timeoutSeconds).until(new Condition(xpath));
        } catch (TimeoutException e) {
            TimeoutException ex = new TimeoutException(notFoundMsg);
            ex.initCause(e);
            throw ex;
        }
    }

    public static class Condition implements ExpectedCondition<Boolean> {

        private String xpath;

        public Condition(String xpath) {
            this.xpath = xpath;
        }

        @Override
        public Boolean apply(WebDriver webDriver) {
            return !webDriver.findElements(By.xpath(xpath)).isEmpty();
        }
    }
}
