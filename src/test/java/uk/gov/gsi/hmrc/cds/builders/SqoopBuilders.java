package uk.gov.gsi.hmrc.cds.builders;


import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.manager.ManagerFactory;
import com.cloudera.sqoop.metastore.JobData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;

public class SqoopBuilders {

    public static class AlwaysDummyFactory extends ManagerFactory {
        public ConnManager accept(JobData data) {
            return new DummyManager();
        }
    }

    public static class DummyManager extends ConnManager {
        public void close() {
        }

        public String[] listDatabases() {
            return null;
        }

        public String[] listTables() {
            String[] tables = new String[]{"cdstable"};
            return tables;
        }

        public String[] getColumnNames(String tableName) {
            return null;
        }

        public String getPrimaryKey(String tableName) {
            return null;
        }

        public String toJavaType(int sqlType) {
            return null;
        }

        public String toHiveType(int sqlType) {
            return null;
        }

        public Map<String, Integer> getColumnTypes(String tableName) {
            return null;
        }

        public ResultSet readTable(String tableName, String[] columns) {
            return null;
        }

        public Connection getConnection() {
            return null;
        }

        public String getDriverClass() {
            return null;
        }

        public void execAndPrint(String s) {
        }

        public void importTable(ImportJobContext context) {
        }

        public void release() {
        }
    }

}
