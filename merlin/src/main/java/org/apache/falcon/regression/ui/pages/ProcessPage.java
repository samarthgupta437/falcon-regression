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
import java.util.Map;
import java.util.Scanner;

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
    private static final String LINEAGE_MODAL = "//div[@id='lineage-modal']";
    private static final String VERTICES_BLOCKS_XPATH = "//*[name() = 'svg']/*[name()" +
        "='g']//*[name() = 'g'][not(@class='lineage-link')]";
    private static final String EDGE_BLOCK_XPATH = "//*[name() = 'svg']//*[name()" +
        "='g'][@class='lineage-link']";
    private static final String CIRCLE_XPATH = "//*[name() = 'circle']";
    private static final String LINEAGE_INFO_PANEL = "//div[@id='lineage-info-panel']";
    private static final String LINEAGE_TITLE = LINEAGE_MODAL +
        "//div[@class='modal-header']/h4";
    private static final String LINEAGE_LEGENDS_BLOCK = LINEAGE_MODAL +
        "//div[@class='modal-body']/div[ul[@class='lineage-legend']]";

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
            waitForElement(LINEAGE_TITLE, 1);
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
            this.navigateTo();
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
     * @return - map of parameters from info panel and their values
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

    /**
     * @return - map of legends as key and their names on UI as values
     */
    public HashMap<String, String> getLegends() {
        HashMap<String, String> map = null;
        if (isLineageOpened) {
            map = new HashMap<String, String>();
            List<WebElement> legends = driver.findElements(By.xpath(LINEAGE_LEGENDS_BLOCK +
                "/ul/li"));
            for (WebElement legend : legends) {
                String value = legend.getText();
                String elementClass = legend.getAttribute("class");
                map.put(elementClass, value);
            }
        }
        return map;
    }

    /**
     * @return - the main title of Lineage UI
     */
    public String getLineageTitle() {
        if (isLineageOpened) {
            return driver.findElement(By.xpath(LINEAGE_TITLE)).getText();
        } else return null;
    }

    /**
     * @return - the name of legends block
     */
    public String getLegendsTitile() {
        if (isLineageOpened) {
            return driver.findElement(By.xpath(LINEAGE_LEGENDS_BLOCK + "/h4")).getText();
        } else return null;
    }

    public int getEdgesNumber() {
        if (isLineageOpened) {
            List<WebElement> edgeBlocks = driver.findElements(By.xpath(EDGE_BLOCK_XPATH));
            return edgeBlocks.size();
        } else {
            return 0;
        }
    }

    public int getCircleRadius() {
        WebElement circle = driver.findElements(By.xpath(VERTICES_BLOCKS_XPATH + CIRCLE_XPATH))
            .get(0);
        return Integer.parseInt(circle.getAttribute("r"));
    }

    public void getStartEndOfEdge() {

    }

    public HashMap<String, int[]> getEdgesEndpoints(HashMap<String, String> startEndInstances) {
        HashMap<String, int[]> map = null;
        if(isLineageOpened) {
            map = new HashMap<String, int[]>();
            for(Map.Entry<String, String> entry : startEndInstances.entrySet()) {
                String startVertex = entry.getKey();
                String endVertex = entry.getValue();
                map.put(startVertex, getVertexCoordinates(startVertex));
                map.put(endVertex, getVertexCoordinates(endVertex));
            }
        }
        return map;
    }

    private int[] getVertexCoordinates(String vertex) {
        int[] coordinates = new int[2];
        /** get circle of start vertex */
        String particularVertexBlock = VERTICES_BLOCKS_XPATH + String.format("[contains(" +
            "., '%s')]", vertex);
        WebElement block = driver.findElement(By.xpath(particularVertexBlock));
        String attribute = block.getAttribute("transform");
        attribute = attribute.replaceAll("[a-zA-Z]", "");
        String [] numbers = attribute.replaceAll("[()]", "").split(",");
        for(int i = 0; i < 2; i++){
            coordinates[i] = Integer.parseInt(numbers[i]);
        }
        return coordinates;
    }



}
