package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;
import java.io.IOException;


public class SqoopClient {

    private static SqoopOptions sqoopOptions = new SqoopOptions();
    private static CdsSqoopTool cdsSqoopTool = new CdsSqoopTool();

    public static void main(String... args) throws IOException{
        runSqoop("src/main/resources/hive-hql","hive.q");
    }

    private static void setUp() {
        sqoopOptions.setConnectString(GenerateHql.CONNECTION_STRING + GenerateHql.DB_NAME);
        sqoopOptions.setUsername(GenerateHql.USERNAME);
        sqoopOptions.setPassword(GenerateHql.PASSWORD);
        sqoopOptions.setDriverClassName(GenerateHql.DRIVER);
        sqoopOptions.setHiveDropDelims(true);
    }

    public static void runSqoop(final String filePath,final String fileName) throws IOException {
        setUp();
        cdsSqoopTool.generateDataVaultHQL(sqoopOptions, filePath, fileName);
    }


}