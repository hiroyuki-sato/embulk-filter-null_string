package org.embulk.filter.null_string;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.HashMap;

import static java.util.Locale.ENGLISH;

public class NullStringVisitorImpl
        implements ColumnVisitor
{
    private static final Logger logger = Exec.getLogger(NullStringFilterPlugin.class);
    private final NullStringFilterPlugin.PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashMap<String, String> nullStringMap = new HashMap<>();

    NullStringVisitorImpl(NullStringFilterPlugin.PluginTask task, Schema inputSchema, Schema outputSchema, PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task = task;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;
        initializeNullStringMap();
    }

    private void initializeNullStringMap()
    {
        for (NullStringFilterPlugin.NullColumnConfig nullStringConfig : task.getNullColumnConfig()) {
            logger.debug(String.format(ENGLISH, "set null_string %s = \"%s\"", nullStringConfig.getName(), nullStringConfig.getNullString()));
            nullStringMap.put(nullStringConfig.getName(), nullStringConfig.getNullString());
        }
    }

    @Override
    public void booleanColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(inputColumn));
    }

    @Override
    public void longColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setLong(outputColumn, pageReader.getLong(inputColumn));
        }
    }

    @Override
    public void doubleColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setDouble(outputColumn, pageReader.getDouble(inputColumn));
        }
    }

    @Override
    public void stringColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            String nullString = nullStringMap.get(outputColumn.getName());
            String value = pageReader.getString(inputColumn);

            logger.debug(String.format(ENGLISH, "value = \"%s\", null_string = \"%s\"", value, nullString));

            if (nullString != null && nullString.equals(value)) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setString(outputColumn, pageReader.getString(inputColumn));
            }
        }
    }

    @Override
    public void jsonColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setJson(outputColumn, pageReader.getJson(inputColumn));
        }
        ;
    }

    @Override
    public void timestampColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
        }
    }
}
