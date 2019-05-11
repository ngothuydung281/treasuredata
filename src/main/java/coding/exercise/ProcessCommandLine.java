package coding.exercise;

import org.apache.commons.cli.*;

public class ProcessCommandLine {
    private TreasureQuery query;
    private ArgsValidator argValidator;
    private String[] args = null;
    private String outputFormat = Constant.tabular;
    private String engine = Constant.Presto;
    private String columnList;
    private String minTime;
    private String maxTime;
    private String limit;
    private String dbName;
    private String tableName;

    public ProcessCommandLine(String[] args) {
        this.args = args;
        query = new TreasureQuery();
        argValidator = new ArgsValidator();
    }

    public void process() throws ParseException, InterruptedException {
        CommandLine cmd;
        try {
            CommandLineParser parser = new DefaultParser();
            cmd = parser.parse(Option.options, args);
        }
        catch (UnrecognizedOptionException ex) {
            System.out.println("Option not supported: " + ex.getOption());
            MyExercise.help();
            return;
        }
        if (isDatabaseAndTableNameValid()) {
            GetAndValidateArgs(cmd);
        }

        if (MyExercise.errorList.isEmpty()) {
            query.runSQL(outputFormat, engine, columnList, minTime, maxTime, limit, dbName, tableName);
        }
        else {
            MyExercise.errorList.forEach(e -> System.out.println(e));
        }
    }

    private boolean isDatabaseAndTableNameValid() {
        if (args.length < 2) {
            MyExercise.errorList.add("ERROR: Must have database name and table name at the end of arguments");
            return false;
        } else {
            dbName = args[args.length - 2];
            if (Option.options.hasOption(dbName)) {
                MyExercise.errorList.add("ERROR: Must have database name and table name at the end of arguments");
                return false;
            } else {
                tableName = args[args.length - 1];
                if (query.isDatabaseExist(dbName)) {
                    if (!query.isTableExist(dbName, tableName)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private void GetAndValidateArgs(CommandLine cmd) {
        // validate output format
        if (cmd.hasOption(Option.optionOutputFormat)) {
            outputFormat = cmd.getOptionValue(Option.optionOutputFormat);
            if (!outputFormat.equalsIgnoreCase(Constant.csv) && !outputFormat.equalsIgnoreCase(Constant.tabular)) {
                MyExercise.errorList.add(String.format("ERROR: Output format -f %s is not supported. Output format -f must be 'csv' or 'tabular'", outputFormat));
            }
        }
        // validate engine
        if (cmd.hasOption(Option.optionEngine)) {
            engine = cmd.getOptionValue(Option.optionEngine);
            if (!engine.equalsIgnoreCase(Constant.Hive) && !engine.equalsIgnoreCase(Constant.Presto)) {
                MyExercise.errorList.add(String.format("ERROR: Engine -e %s is not supported. Engine -e must be 'Hive' or 'Presto'", engine));
            }
        }
        // validate column
        if (cmd.hasOption(Option.optionColumn)) {
            columnList = cmd.getOptionValue(Option.optionColumn).replace("'", "");
            if (columnList == null) {
                MyExercise.errorList.add("ERROR: Must have value for argument -c to define column list");
            } else {
                query.checkColumnValid(dbName, tableName, columnList);
            }
        }
        // validate min time
        boolean minTimeValid = true;
        if (cmd.hasOption(Option.optionMinTime)) {
            minTime = cmd.getOptionValue(Option.optionMinTime);
            if (minTime.equalsIgnoreCase("NULL"))
                minTime = null;
            else
                minTimeValid = argValidator.isDigitOnly(minTime, "minimum time");
        }
        // validate max time
        boolean maxTimeValid = true;
        if (cmd.hasOption(Option.optionMaxTime)) {
            maxTime = cmd.getOptionValue(Option.optionMaxTime);
            if (maxTime.equalsIgnoreCase("NULL"))
                maxTime = null;
            else
                maxTimeValid = argValidator.isDigitOnly(maxTime, "maximum time");
            if (minTime != null && maxTime != null && minTimeValid && maxTimeValid) {
                argValidator.checkTimeValid(minTime, maxTime);
            }
        }
        // validate limit
        if (cmd.hasOption(Option.optionLimit)) {
            limit = cmd.getOptionValue(Option.optionLimit);
            argValidator.isDigitOnly(limit, "limit");
        }
    }
}
