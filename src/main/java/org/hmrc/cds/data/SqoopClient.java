package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;
import java.io.IOException;


public class SqoopClient {

    private SqoopOptions sqoopOptions = new SqoopOptions();
    private CdsSqoopTool cdsSqoopTool = new CdsSqoopTool();

    private void setUp() {
        sqoopOptions.setConnectString(GenerateHql.CONNECTION_STRING + GenerateHql.DB_NAME);
        sqoopOptions.setUsername(GenerateHql.USERNAME);
        sqoopOptions.setPassword(GenerateHql.PASSWORD);
        sqoopOptions.setDriverClassName(GenerateHql.DRIVER);
        sqoopOptions.setHiveDropDelims(true);
    }

    public void runSqoop(final String filePath,final String fileName) throws IOException {
        setUp();
        cdsSqoopTool.generateDataVaultHQL(sqoopOptions, filePath, fileName);
    }


}