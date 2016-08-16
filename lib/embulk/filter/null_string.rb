Embulk::JavaPlugin.register_filter(
  "null_string", "org.embulk.filter.null_string.NullStringFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
