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

        Configuration conf = new Configuration();
        SqoopOptions options = new SqoopOptions();
        TableDefWriter writer = new TableDefWriter(options, null,
                table, table, conf, false);

        Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();
        writer.setColumnTypes(colTypes);

        cdsSqoopTool.setTableWriter(writer);
        List<String> statements = cdsSqoopTool.generateTableHQL(options, table);
        assertTrue(statements.size() == 2);
        assertEquals("DROP TABLE IF EXISTS table;", statements.get(0));
        assertTrue(statements.get(1).indexOf(
                "CREATE TABLE IF NOT EXISTS `" + table + "`") != -1);


        final JobData jobData = new JobData(options, cdsSqoopTool);
        conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY, SqoopBuilders.AlwaysDummyFactory.class.getName());

        ConnFactory factory = new ConnFactory(conf);

        ConnManager manager = factory.getManager(new JobData(new SqoopOptions(), cdsSqoopTool));
        cdsSqoopTool.setManager(manager);

        List<String> allTables = cdsSqoopTool.getAllTables(options);
        System.out.println("Tables Size: " + allTables.size());
    }

    @Test
    public void testGetAllTables() throws Exception {
        Configuration conf = new Configuration();
        SqoopOptions options = new SqoopOptions();

        final JobData jobData = new JobData(options, cdsSqoopTool);
        conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY, SqoopBuilders.AlwaysDummyFactory.class.getName());

        ConnFactory factory = new ConnFactory(conf);

        ConnManager manager = factory.getManager(new JobData(new SqoopOptions(), cdsSqoopTool));
        cdsSqoopTool.setManager(manager);

        List<String> allTables = cdsSqoopTool.getAllTables(options);
        assertEquals(1,allTables.size());
    }

}
