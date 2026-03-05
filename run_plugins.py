from stock_datasource.core.plugin_manager import plugin_manager
import logging

logging.basicConfig(level=logging.INFO)
plugin_manager.discover_plugins()

for plugin_name in ["tushare_index_daily"]:
    print(f"Running {plugin_name}...")
    plugin = plugin_manager.get_plugin(plugin_name)
    if plugin:
        result = plugin.run(trade_date="20240301")
        print(f"{plugin_name} result: {result}")
    else:
        print(f"Plugin {plugin_name} not found")
