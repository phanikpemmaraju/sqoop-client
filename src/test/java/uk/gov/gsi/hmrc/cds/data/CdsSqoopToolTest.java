package uk.gov.gsi.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hive.TableDefWriter;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.ConnFactory;
import org.apache.sqoop.util.SqlTypeMap;
import org.hmrc.cds.data.CdsSqoopTool;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gsi.hmrc.cds.builders.SqoopBuilders;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class CdsSqoopToolTest {

    private CdsSqoopTool cdsSqoopTool;

    @Before
    public void setUp() {
        cdsSqoopTool = new CdsSqoopTool();
    }

    @Test
    public void testGenerateTableHQL() throws Exception {
        String table = "table";
        final String HIVE_DATABASE_NAME_CLASSIFIER = "${DATAVAULT_DB}";
        Configuration conf = new Configuration();
        SqoopOptions options = new SqoopOptions();
        options.setHiveDatabaseName(HIVE_DATABASE_NAME_CLASSIFIER);
        TableDefWriter writer = new TableDefWriter(options, null,
                table, table, conf, false);

        Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();
        writer.setColumnTypes(colTypes);

        cdsSqoopTool.setTableWriter(writer);
        List<String> statements = cdsSqoopTool.generateTableHQL(options, table);
        assertTrue(statements.size() == 3);
        assertEquals("DROP TABLE IF EXISTS `" + HIVE_DATABASE_NAME_CLASSIFIER + "`." + table + ";", statements.get(0));
        assertTrue(statements.get(1).indexOf(
                "CREATE TABLE IF NOT EXISTS `" + HIVE_DATABASE_NAME_CLASSIFIER + "`.`" + table + "`") != -1);
        assertEquals("",statements.get(2));
    }

    @Test
    public void testGetAllTables() throws Exception {
        Configuration conf = new Configuration();
        SqoopOptions options = new SqoopOptions();

        conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY, SqoopBuilders.AlwaysDummyFactory.class.getName());

        ConnFactory factory = new ConnFactory(conf);

        ConnManager manager = factory.getManager(new JobData(new SqoopOptions(), cdsSqoopTool));
        cdsSqoopTool.setManager(manager);

        List<String> allTables = cdsSqoopTool.getAllTables(options);
        assertEquals(1,allTables.size());
    }

}
