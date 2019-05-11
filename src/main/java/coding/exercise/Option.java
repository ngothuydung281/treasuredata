package coding.exercise;

import org.apache.commons.cli.Options;

public class Option
{
    static Options options = new Options();
    static String optionOutputFormat = "f";
    static String optionEngine = "e";
    static String optionColumn = "c";
    static String optionMinTime = "m";
    static String optionMaxTime = "M";
    static String optionLimit = "l";

    public static void initCommandOption()
    {
        options.addOption(optionOutputFormat, true, "output format: csv or tabular");
        options.addOption(optionEngine, true, "engine: presto or hive");
        options.addOption(optionColumn, true, "column");
        options.addOption(optionMinTime, true, "minimum time in unix timestamp or NULL");
        options.addOption(optionMaxTime, true, "maximum time in unix timestamp or NULL");
        options.addOption(optionLimit, true, "query limit");
    }
}
