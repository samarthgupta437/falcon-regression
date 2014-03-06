package org.apache.falcon.regression.core.util;

public class OSUtil {
    public static String SEPARATOR = System.getProperty("file.separator", "/");

    public static String RESOURCES = String.format("src%stest%sresources", SEPARATOR, SEPARATOR);

    public static String RESOURCES_OOZIE = String.format(RESOURCES + "%soozie", SEPARATOR);

    public static String WORKFLOW = String.format(RESOURCES_OOZIE + "%sworkflow.xml", SEPARATOR);

    public static String OOZIE_EXAMPLES = String.format(RESOURCES_OOZIE +
            "%slib%soozie-examples-3.1.5.jar", SEPARATOR, SEPARATOR);

    public static String RESOURCES_PIG = String.format(RESOURCES + "%spig", SEPARATOR);

    public static String OOZIE_EXAMPLE_INPUT_DATA = String.format(RESOURCES +
            "%sOozieExampleInputData", SEPARATOR);

    public static String NORMAL_INPUT = String.format(OOZIE_EXAMPLE_INPUT_DATA
            + "%snormalInput", SEPARATOR);

    public static String LATE_DATA = String.format(OOZIE_EXAMPLE_INPUT_DATA + "%slateData", SEPARATOR);

    public static String SECOND_LATE_DATA = String.format
            (OOZIE_EXAMPLE_INPUT_DATA + "%s2ndLateData", SEPARATOR);

    public static String EL_BUNDLE = String.format(RESOURCES + "%sELbundle", SEPARATOR);

    public static String IVORY_CLI = String.format(RESOURCES + "%sIvoryClient%sIvoryCLI.jar",
            SEPARATOR, SEPARATOR);

}
