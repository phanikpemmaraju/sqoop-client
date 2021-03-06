package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;

import java.io.IOException;
import java.sql.SQLException;


public class SqoopClient {

    private static final String HIVE_DATABASE_NAME_CLASSIFIER = "${DATAVAULT_DB}";
    private static SqoopOptions sqoopOptions = new SqoopOptions();
    private static CdsSqoopTool cdsSqoopTool = new CdsSqoopTool();

    public static void main(String... args) throws SQLException, IOException, ClassNotFoundException {
        Boolean checkPopulateDataFlag = Boolean.valueOf(System.getProperty("populate"));

        if (!checkPopulateDataFlag)
            populateDataVault();
        else
            populateDataVault();

    }

    private static void setUp() {
        sqoopOptions.setConnectString(GenerateHql.CONNECTION_STRING + GenerateHql.DB_NAME);
        sqoopOptions.setUsername(GenerateHql.USERNAME);
        sqoopOptions.setPassword(GenerateHql.PASSWORD);
        sqoopOptions.setDriverClassName(GenerateHql.DRIVER);
        sqoopOptions.setHiveDropDelims(true);
        sqoopOptions.setHiveDatabaseName(HIVE_DATABASE_NAME_CLASSIFIER);
    }

    public static void runSqoop(final String filePath, final String fileName) throws SQLException, IOException, ClassNotFoundException {
        setUp();
        cdsSqoopTool.generateDataVaultHQL(sqoopOptions, filePath, fileName);
    }

    public static void populateDataVault() throws IOException, SQLException, ClassNotFoundException {
        cdsSqoopTool.createAndPopulateDataVault();
    }

}