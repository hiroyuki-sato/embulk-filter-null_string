package org.embulk.filter.null_string;

import org.embulk.config.Config;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

import java.util.List;

public class NullStringFilterPlugin
        implements FilterPlugin
{
    public interface NullColumnConfig
            extends Task
    {
        @Config("name")
        String getName();

        @Config("null_string")
        String getNullString();
    }

    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public List<NullColumnConfig> getNullColumnConfig();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        for (NullColumnConfig nullConfig : task.getNullColumnConfig()) {
            inputSchema.lookupColumn(nullConfig.getName());
        }

        control.run(task.dump(), outputSchema);
    }

    static Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        Schema.Builder builder = Schema.builder();
        for (Column inputColumns : inputSchema.getColumns()) {
            builder.add(inputColumns.getName(), inputColumns.getType());
        }
        return builder.build();
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput()
        {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private NullStringVisitorImpl visitor = new NullStringVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
