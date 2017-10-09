package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class SqoopClient {

    private SqoopOptions SqoopOptions = new SqoopOptions();
    private CustomsSqoopTool customsSqoopTool = new CustomsSqoopTool();
    private Logger LOG = LoggerFactory.getLogger(SqoopClient.class);

    private void setUp() {
        String connection_string = GenerateSql.CONNECTION_STRING + GenerateSql.DB_NAME;
        LOG.info("Database connection String: "+ connection_string);
        SqoopOptions.setConnectString(connection_string);
        SqoopOptions.setUsername(GenerateSql.USERNAME);
        SqoopOptions.setPassword(GenerateSql.PASSWORD);
        SqoopOptions.setDriverClassName(GenerateSql.DRIVER);
}

    public void runSqoop(final String filePath,final String fileName) throws IOException {
        setUp();
        customsSqoopTool.generateDataVaultHQL(SqoopOptions, filePath, fileName);
    }


}