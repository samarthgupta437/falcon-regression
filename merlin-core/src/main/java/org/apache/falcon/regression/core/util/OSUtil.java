package org.apache.falcon.regression.core.util;

public class OSUtil {

    public static String SEPARATOR = System.getProperty("file.separator", "/");
    public static String RESOURCES = String.format("src%stest%sresources", SEPARATOR, SEPARATOR);
    public static String RESOURCES_OOZIE = String.format(RESOURCES + "%soozie", SEPARATOR);
    public static String OOZIE_EXAMPLE_INPUT_DATA =
            String.format(RESOURCES + "%sOozieExampleInputData", SEPARATOR);
    public static String NORMAL_INPUT =
            String.format(OOZIE_EXAMPLE_INPUT_DATA + "%snormalInput", SEPARATOR);

}
