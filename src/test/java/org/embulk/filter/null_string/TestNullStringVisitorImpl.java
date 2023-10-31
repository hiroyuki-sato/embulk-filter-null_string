package org.embulk.filter.null_string;

import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.null_string.NullStringFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.PageTestUtils;
import org.embulk.test.TestPageBuilderReader;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.ValueFactory;

import java.util.List;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class TestNullStringVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    @Before
    public void createResource()
    {
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(ExecInternal.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }

    private List<Object[]> filter(PluginTask task, Schema inputSchema, Object... objects)
    {
        TestPageBuilderReader.MockPageOutput output = new TestPageBuilderReader.MockPageOutput();
        Schema outputSchema = NullStringFilterPlugin.buildOutputSchema(task, inputSchema);
        PageBuilder pageBuilder = new PageBuilder(runtime.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        NullStringVisitorImpl visitor = new NullStringVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, objects);
        for (Page page : pages) {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                outputSchema.visitColumns(visitor);
                pageBuilder.addRecord();
            }
        }
        pageBuilder.finish();
        pageBuilder.close();
        return Pages.toObjects(outputSchema, output.pages);
    }

    @Test
    public void visit_nullString_NoConvert()
    {
        PluginTask task = taskFromYamlString(
                "type: null_string",
                "columns: []");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"),
                // row2
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(6, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
        }
    }

    @Test
    public void visit_nullString_BasicConvert()
    {
        PluginTask task = taskFromYamlString(
                "type: null_string",
                "columns:",
                "  - name: null_string",
                "    null_string: \"\"");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("null_string", STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "",
                // row2
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(7, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
            assertEquals(null, record[6]);
        }
    }

    @Test
    public void visit_nullString_NoMatchConvert()
    {
        PluginTask task = taskFromYamlString(
                "type: null_string",
                "columns:",
                "  - name: null_string",
                "    null_string: \"\"");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("null_string", STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "test",
                // row2
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "test");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(7, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
            assertEquals("test", record[6]);
        }
    }

    @Test
    public void visit_nullString_StringConvert()
    {
        PluginTask task = taskFromYamlString(
                "type: null_string",
                "columns:",
                "  - name: null_string",
                "    null_string: \"_NULL_\"");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("null_string", STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "_NULL_",
                // row2
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "_NULL_");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(7, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
            assertEquals(null, record[6]);
        }
    }
    @Test
    public void visit_nullString_StringConvert2()
    {
        PluginTask task = taskFromYamlString(
                "type: null_string",
                "columns:",
                "  - name: null_string",
                "    null_string: \"\\\\N\"");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("null_string", STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "\\N",
                // row2
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "\\N");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(7, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
            assertEquals(null, record[6]);
        }
    }
}

