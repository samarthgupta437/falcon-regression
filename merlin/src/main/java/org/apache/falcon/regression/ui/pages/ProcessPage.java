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
import org.testng.Assert;
import org.openqa.selenium.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ProcessPage extends EntityPage<Process> {

    private Logger logger = Logger.getLogger(ProcessPage.class);
    private final static String INSTANCES_PANEL = "//div[@id='panel-instance']//span";
    private final static String INSTANCE_STATUS_TEMPLATE =
        "//div[@id='panel-instance']//span[contains(..,'%s')]";
    private final static String LINEAGE_LINK_TEMPLATE =
        "//a[@class='lineage-href' and @data-instance-name='%s']";

    public ProcessPage(WebDriver driver, PrismHelper helper, String entityName) {
        super(driver, helper, ENTITY_TYPE.PROCESS, Process.class, entityName);
    }

    private boolean isLineageOpened = false;

    private static final String LINE_AGE_BUTTON_XPATH =
        "//div[@id='panel-instance']//table/tbody/tr/td[contains(.., " +
            "'%s')]/a[contains(., 'Lineage')]";
    private static final String CLOSE_LINE_AGE_BUTTON_XPATH =
        "//body[@class='modal-open']//button[contains(., 'Close')]";
    private static final String LINEAGE_MODAL = "//div[@id='lineage-modal']";
    private static final String SVG_XPATH = "//*[name() = 'svg']";
    private static final String G_XPATH = "//*[name()='g']";
    private static final String VERTICES_BLOCKS_XPATH = SVG_XPATH + G_XPATH +
        G_XPATH + "[not(@class='lineage-link')]";
    private static final String EDGE_XPATH = SVG_XPATH + G_XPATH + "[@class='lineage-link']" +
        "//*[name()='path']";
    private static final String CIRCLE_XPATH = "//*[name() = 'circle']";
    private static final String LINEAGE_INFO_PANEL = "//div[@id='lineage-info-panel']";
    private static final String LINEAGE_TITLE = LINEAGE_MODAL +
        "//div[@class='modal-header']/h4";
    private static final String LINEAGE_LEGENDS_BLOCK = LINEAGE_MODAL +
        "//div[@class='modal-body']/div[ul[@class='lineage-legend']]";

    /**
     * @param nominalTime particular instance of process, defined by it's start time
     */
    public void openLineage(String nominalTime) {
        waitForElement(String.format(LINE_AGE_BUTTON_XPATH, nominalTime), DEFAULT_TIMEOUT,
            "Lineage button didn't appear");
        logger.info("Working with instance: " + nominalTime);
        WebElement lineage =
            driver.findElement(By.xpath(String.format(LINE_AGE_BUTTON_XPATH, nominalTime)));
        logger.info("Opening lineage...");
        lineage.click();
        waitForElement(VERTICES_BLOCKS_XPATH + CIRCLE_XPATH, DEFAULT_TIMEOUT,
            "Circles not found");
        waitForDisplayed(LINEAGE_TITLE, DEFAULT_TIMEOUT, "Lineage title not found");
        isLineageOpened = true;
    }

    public void closeLineage() {
        if (isLineageOpened) {
            WebElement close = driver.findElement(By.xpath(CLOSE_LINE_AGE_BUTTON_XPATH));
            close.click();
            isLineageOpened = false;
            waitForDisappear(CLOSE_LINE_AGE_BUTTON_XPATH, DEFAULT_TIMEOUT,
                "Lineage didn't disappear");
        }
    }

    @Override
    public void refresh() {
        super.refresh();
        isLineageOpened = false;
    }

    /**
     * @return map with instances names and their nominal start time
     */
    public HashMap<String, List<String>> getAllVertices() {
        HashMap<String, List<String>> map = null;
        if (isLineageOpened) {
            waitForElement(VERTICES_BLOCKS_XPATH + "[contains(.,'/')]", DEFAULT_TIMEOUT,
                "Vertices blocks with names not found");
            List<WebElement> blocks = driver.findElements(By.xpath(VERTICES_BLOCKS_XPATH));
            map = new HashMap<String, List<String>>();
            for (WebElement block : blocks) {
                String text = block.getText();
                Assert.assertTrue(text.contains("/"), "Expecting text to contain /: " + text);
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
     * @return list of all vertices names
     */
    public List<String> getAllVerticesNames() {
        List<String> list = new ArrayList<String>();
        if (isLineageOpened) {
            waitForElement(CLOSE_LINE_AGE_BUTTON_XPATH, DEFAULT_TIMEOUT,
                "Close Lineage button not found");
            waitForElement(VERTICES_BLOCKS_XPATH, DEFAULT_TIMEOUT,
                "Vertices not found");
            List<WebElement> blocks = driver.findElements(By.xpath(VERTICES_BLOCKS_XPATH));
            for (WebElement block : blocks) {
                list.add(block.getText());
            }
        }
        return list;
    }

    /**
     * Vertex is defined by it's entity name and particular time of it's creation
     */
    public void clickOnVertex(String entityName, String nominalTime) {
        if (isLineageOpened) {
            String particularBlock =
                String.format("[contains(., '%s/%s')]", entityName, nominalTime);
            WebElement circle = driver.findElement(By.xpath(VERTICES_BLOCKS_XPATH +
                particularBlock + CIRCLE_XPATH));
            Actions builder = new Actions(driver);
            builder.click(circle).build().perform();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                logger.info("Sleep was interrupted");
            }
        }
    }

    /**
     * @return map of parameters from info panel and their values
     */
    public HashMap<String, String> getPanelInfo() {
        HashMap<String, String> map = null;
        if (isLineageOpened) {
            //check if vertex was clicked
            waitForElement(LINEAGE_INFO_PANEL + "//div[@class='col-md-3']", DEFAULT_TIMEOUT,
                "Lineage info panel not found");
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
     * @return map of legends as key and their names on UI as values
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
     * @return the main title of Lineage UI
     */
    public String getLineageTitle() {
        if (isLineageOpened) {
            return driver.findElement(By.xpath(LINEAGE_TITLE)).getText();
        } else return null;
    }

    /**
     * @return the name of legends block
     */
    public String getLegendsTitile() {
        if (isLineageOpened) {
            return driver.findElement(By.xpath(LINEAGE_LEGENDS_BLOCK + "/h4")).getText();
        } else return null;
    }

    /**
     * @return list of edges present on UI. Each edge presented as two 2d points - beginning and
     * the end of the edge.
     */
    public List<Point[]> getEdgesFromGraph() {
        List<Point[]> pathsEndpoints = null;
        if (isLineageOpened) {
            pathsEndpoints = new ArrayList<Point[]>();
            List<WebElement> paths = driver.findElements(By.xpath(EDGE_XPATH));
            for (WebElement path : paths) {
                String d = path.getAttribute("d");
                d = d.replaceAll("[MLC]", ",");
                String[] coordinates = d.split(",");
                int x = 0, y, i = 0;
                while (i < coordinates.length) {
                    if (!coordinates[i].isEmpty()) {
                        x = (int) Double.parseDouble(coordinates[i]);
                        break;
                    } else {
                        i++;
                    }
                }
                y = (int) Double.parseDouble(coordinates[i + 1]);
                Point startPoint = new Point(x, y);
                x = (int) Math.round(Double.parseDouble(coordinates[coordinates.length - 2]));
                y = (int) Math.round(Double.parseDouble(coordinates[coordinates.length - 1]));
                Point endPoint = new Point(x, y);
                pathsEndpoints.add(new Point[]{startPoint, endPoint});
            }
        }
        return pathsEndpoints;
    }

    /**
     * @return common value for radius of every vertex (circle) on the graph
     */
    public int getCircleRadius() {
        WebElement circle = driver.findElements(By.xpath(VERTICES_BLOCKS_XPATH + CIRCLE_XPATH))
            .get(0);
        return Integer.parseInt(circle.getAttribute("r"));
    }

    /**
     * Finds vertex on the graph by its name and evaluates its coordinates as 2d point
     * @param vertex the name of vertex which point is needed
     * @return Point(x,y) object
     */
    public Point getVertexEndpoint(String vertex) {
        /** get circle of start vertex */
        String particularVertexBlock = VERTICES_BLOCKS_XPATH + String.format("[contains(" +
            "., '%s')]", vertex);
        WebElement block = driver.findElement(By.xpath(particularVertexBlock));
        String attribute = block.getAttribute("transform");
        attribute = attribute.replaceAll("[a-zA-Z]", "");
        String[] numbers = attribute.replaceAll("[()]", "").split(",");
        return new Point(Integer.parseInt(numbers[0]), Integer.parseInt(numbers[1]));
    }

    public String getInstanceStatus(String instanceDate) {
        waitForInstancesPanel();
        List<WebElement> status =
            driver.findElements(By.xpath(String.format(INSTANCE_STATUS_TEMPLATE, instanceDate)));
        if (status.isEmpty()) {
            return null;
        } else {
            return status.get(0).getAttribute("class").replace("instance-icons instance-link-", "");
        }
    }

    public boolean isLineageLinkPresent(String instanceDate) {
        waitForInstancesPanel();
        List<WebElement> lineage =
            driver.findElements(By.xpath(String.format(LINEAGE_LINK_TEMPLATE, instanceDate)));
        return !lineage.isEmpty();
    }

    private void waitForInstancesPanel() {
        waitForElement(INSTANCES_PANEL, DEFAULT_TIMEOUT, "Instances panel didn't appear");
    }

    /**
     * Checks whether vertex is terminal or not
     * @param vertexName name of vertex
     * @return whether it is terminal or not
     */
    public boolean isTerminal(String vertexName) {
        String particularBlock =
            String.format("[contains(., '%s')]", vertexName);
        waitForElement(VERTICES_BLOCKS_XPATH + particularBlock + CIRCLE_XPATH, DEFAULT_TIMEOUT,
            "Vertex not found");
        WebElement vertex = driver.findElement(By.xpath(VERTICES_BLOCKS_XPATH +
            particularBlock + CIRCLE_XPATH));
        String vertexClass = vertex.getAttribute("class");
        return vertexClass.contains("lineage-node-terminal");
    }
}
