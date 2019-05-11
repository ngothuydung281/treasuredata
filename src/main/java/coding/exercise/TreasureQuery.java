package coding.exercise;

import com.google.common.io.CharStreams;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.model.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class TreasureQuery {
    public void runSQL(String outputFormat, String engine, String columnList, String minTime, String maxTime, String limit, String dbName, String tableName) throws InterruptedException {

        String queryString = BuildQueryString(columnList, minTime, maxTime, limit, tableName);
        String jobId;
        if (engine.equalsIgnoreCase(Constant.Presto)) {
            jobId = MyExercise.client.submit(TDJobRequest.newPrestoQuery(dbName, queryString));
        } else {
            jobId = MyExercise.client.submit(TDJobRequest.newHiveQuery(dbName, queryString));
        }
        waitUntilJobFinish(jobId);

        TDJob jobInfo = MyExercise.client.jobInfo(jobId);
        if (!jobInfo.getStatus().equals(TDJob.Status.SUCCESS)) {
            System.out.println(String.format("Status of jobId %s is %s", jobId, jobInfo.getStatus()));
            System.out.println("Query string: " + jobInfo.getQuery());
            System.out.println("Debug log: " + jobInfo.getDebug().toString());
        }
        else {
            if (outputFormat.equalsIgnoreCase(Constant.csv)) {
                OutputFormatCSV(jobId);
            }
            else {
                OutputFormatTabular(jobId);
            }
            System.out.println(String.format("Query result: %s records", jobInfo.getNumRecords()));
        }
    }

    public boolean isDatabaseExist(String dbName) {
        if (!MyExercise.client.existsDatabase(dbName)) {
            MyExercise.errorList.add("Database " + dbName + " not exist");
            return false;
        }
        return true;
    }

    public boolean isTableExist(String dbName, String tableName) {
        if (!MyExercise.client.existsTable(dbName, tableName))
        {
            MyExercise.errorList.add(String.format("Table %s not exist in database %s", tableName, dbName));
            return false;
        }
        return true;
    }

    public void checkColumnValid(String dbName, String tableName, String columnList) {
        TDTable table = MyExercise.client.listTables(dbName).stream().filter(t -> t.getName().equals(tableName)).findFirst().get();
        List<TDColumn> tableColumns = table.getColumns();

        String[] argColumns = columnList.split(",");
        Arrays.stream(argColumns).forEach(c -> {
            if (!tableColumns.stream().anyMatch(tc -> tc.getName().contains(c)) && !c.equalsIgnoreCase("time")) {
                MyExercise.errorList.add(String.format("ERROR: Column name '%s' does not exist in table '%s'", c, tableName));
            }
        });
    }

    private String BuildQueryString(String columnList, String minTime, String maxTime, String limit, String tableName) {
        String select;
        String where = "";
        String limitation = "";

        if (columnList == null) {
            select = "Select *";
        } else {
            select = "Select " + columnList;
        }

        if (minTime == null && maxTime != null) {
            where = String.format(" where TD_TIME_RANGE(time, 0, %s) ", maxTime);
        } else if (minTime != null && maxTime == null) {
            long currentUnixTime = System.currentTimeMillis() / 1000L;
            where = String.format(" where TD_TIME_RANGE(time, %s, %s) ", minTime, currentUnixTime);
        } else if (minTime != null && maxTime != null) {
            where = String.format(" where TD_TIME_RANGE(time, %s, %s) ", minTime, maxTime);
        }

        if (limit != null) {
            limitation = " limit " + limit;
        }

        String query = select + " from " + tableName + where + limitation;
        return query;
    }

    private void OutputFormatCSV(String jobId) {
        MyExercise.client.jobResult(jobId, TDResultFormat.CSV, (InputStream input) ->
        {
            try {
                String result = CharStreams.toString(new InputStreamReader(input));
                System.out.println(result);
                return result;
            } catch (Exception e) {
                System.out.print("Error while trying to output to csv format");
                return 1;
            }
        });
    }

    private void OutputFormatTabular(String jobId) {
        MyExercise.client.jobResult(jobId, TDResultFormat.TSV, (InputStream input) ->
        {
            try {
                String result = CharStreams.toString(new InputStreamReader(input));
                System.out.println(result);
                return result;
            } catch (Exception e) {
                System.out.print("Error while trying to output to tabular format");
                return 1;
            }
        });
    }

    private void waitUntilJobFinish(String jobId) {
        try {
            // Wait until the query finishes
            ExponentialBackOff backoff = new ExponentialBackOff();
            TDJobSummary job = MyExercise.client.jobStatus(jobId);
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backoff.nextWaitTimeMillis());
                job = MyExercise.client.jobStatus(jobId);
            }
        } catch (Exception e) {
            System.out.println("Waiting too long for query of jobId: " + jobId);
        }
    }
}
