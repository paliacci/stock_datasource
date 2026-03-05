
import os
import sys
from pathlib import Path
from datetime import datetime

# Add src to sys.path
sys.path.insert(0, str(Path(os.getcwd()) / "src"))

from stock_datasource.core.plugin_manager import plugin_manager
from stock_datasource.models.database import db_client

def main():
    plugin_manager.discover_plugins()
    plugin = plugin_manager.get_plugin("tushare_index_daily")
    if not plugin:
        print("✗ Plugin tushare_index_daily not found")
        return
    
    print(f"Running {plugin.name} (version {plugin.version})")
    result = plugin.run(trade_date="20240301")
    print(f"Result: {result}")

if __name__ == "__main__":
    main()
