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
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ProcessPage extends EntityPage<Process> {
    private Logger logger = Logger.getLogger(ProcessPage.class);

    public ProcessPage(WebDriver driver, PrismHelper helper, String entityName) {
        super(driver, helper, ENTITY_TYPE.PROCESS, Process.class, entityName);
    }

    private boolean isLineageOpened = false;

    private static final String LINE_AGE_BUTTON_XPATH =
        "//div[@id='panel-instance']//table/tbody/tr/td[contains(.., " +
            "'%s')]/a[contains(., 'Lineage')]";
    private static final String CLOSE_LINE_AGE_BUTTON_XPATH =
        "//div[@class='modal-footer']/button" +
            "[contains(., 'Close')]";
    private static final String VERTICES_BLOCKS_XPATH = "//*[name() = 'svg']/*[name()" +
        "='g']//*[name() = 'g'][not(@class='lineage-link')]";
    private static final String CIRCLE_XPATH = "//*[name() = 'circle']";
    private static final String LINEAGE_INFO_PANEL = "//div[@id='lineage-info-panel']";

    /**
     * @param nominalTime - particular instance of process, defined by it's start time
     */
    public boolean openLineage(String nominalTime) throws InterruptedException {
        waitForElement(String.format(LINE_AGE_BUTTON_XPATH, nominalTime), 5);
        logger.info("Working with instance: " + nominalTime);
        WebElement lineage =
            driver.findElement(By.xpath(String.format(LINE_AGE_BUTTON_XPATH, nominalTime)));
        if (lineage != null) {
            logger.info("Opening lineage...");
            lineage.click();
            waitForElement(VERTICES_BLOCKS_XPATH + CIRCLE_XPATH, 3);
            isLineageOpened = true;
        } else {
            logger.info("Lineage button not found");
        }
        return isLineageOpened;
    }

    public void closeLineage() {
        if (isLineageOpened) {
            WebElement close = driver.findElement(By.xpath(CLOSE_LINE_AGE_BUTTON_XPATH));
            close.click();
            isLineageOpened = false;
        }
    }

    /**
     * @return map with instances names and their nominal start time
     */
    public HashMap<String, List<String>> getAllVertices() {
        HashMap<String, List<String>> map = null;
        if (isLineageOpened) {
            waitForElement(CLOSE_LINE_AGE_BUTTON_XPATH, 5);
            List<WebElement> blocks = driver.findElements(By.xpath(VERTICES_BLOCKS_XPATH));
            map = new HashMap<String, List<String>>();
            for (WebElement block : blocks) {
                String text = block.getText();
                String[] separate = text.split("/");
                String name = separate[0];
                String nominalTime = separate[1];
                if (map.containsKey(name)) {
                    map.get(name).add(nominalTime);
                } else {
                    List<String> instances = new ArrayList<String>();
                    instances.add(nominalTime);
                    map.put(name, instances);
                }
            }
        }
        return map;
    }

    /**
     * Vertex is defined by it's entity name and particular time of it's creation
     */
    public void clickOnVertex(String entityName, String nominalTime) throws InterruptedException {
        if (isLineageOpened) {
            String particularBlock =
                String.format("[contains(., '%s/%s')]", entityName, nominalTime);
            WebElement circle = driver.findElement(By.xpath(VERTICES_BLOCKS_XPATH +
                particularBlock + CIRCLE_XPATH));
            Actions builder = new Actions(driver);
            builder.click(circle).build().perform();
            Thread.sleep(500);
        }
    }

    /**
     * @return
     */
    public HashMap<String, String> getPanelInfo() {
        HashMap<String, String> map = null;
        if (isLineageOpened) {
            //check if vertex was clicked
            waitForElement(LINEAGE_INFO_PANEL + "//div[@class='col-md-3']", 3);
            List<WebElement> infoBlocks =
                driver.findElements(By.xpath(LINEAGE_INFO_PANEL + "//div[@class='col-md-3']"));
            map = new HashMap<String, String>();
            for (WebElement infoBlock : infoBlocks) {
                String text = infoBlock.getText();
                String[] values = text.split("\n");
                map.put(values[0], values[1]);
            }
        }
        return map;
    }
}
