package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;

import java.io.File;
import java.io.IOException;


public class SqoopClient {

    private SqoopOptions SqoopOptions = new SqoopOptions();
    private CustomsSqoopTool customsSqoopTool = new CustomsSqoopTool();

    private void setUp() {
        SqoopOptions.setConnectString(GenerateSql.CONNECTION_STRING + File.separator + GenerateSql.DB_NAME);
        SqoopOptions.setUsername(GenerateSql.USERNAME);
        SqoopOptions.setPassword(GenerateSql.PASSWORD);
        SqoopOptions.setDriverClassName(GenerateSql.DRIVER);
}

    public void runSqoop(final String filePath,final String fileName) throws IOException {
        setUp();
        customsSqoopTool.generateDataVaultHQL(SqoopOptions, filePath, fileName);
    }


}