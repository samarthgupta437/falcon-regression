package org.apache.falcon.regression.core.util;

public class OSUtil {

    public static String SEPARATOR = System.getProperty("file.separator", "/");
    public static String RESOURCES = String.format("src%stest%sresources%s", SEPARATOR, SEPARATOR, SEPARATOR);
    public static String RESOURCES_OOZIE = String.format(RESOURCES + "oozie%s", SEPARATOR);
    public static String OOZIE_EXAMPLE_INPUT_DATA =
            String.format(RESOURCES + "OozieExampleInputData%s", SEPARATOR);
    public static String NORMAL_INPUT =
            String.format(OOZIE_EXAMPLE_INPUT_DATA + "normalInput%s", SEPARATOR);

    public static String getPath(String... pathParts) {
        StringBuilder path = new StringBuilder();
        if (pathParts.length == 0) return "";

        path.append(pathParts[0]);
        for (int i = 1; i < pathParts.length; i++) {
            path.append(OSUtil.SEPARATOR);
            path.append(pathParts[i]);
        }
        return path.toString();
    }
}
