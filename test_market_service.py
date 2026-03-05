
import os
import sys
import asyncio
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(os.getcwd()) / "src"))

from stock_datasource.modules.market.service import MarketService

async def main():
    service = MarketService()
    print("Fetching market overview...")
    overview = await service.get_market_overview()
    print(f"Overview: {overview}")
    
    print("\nFetching hot stocks...")
    hot_stocks = await service.get_hot_stocks()
    print(f"Hot Stocks count: {len(hot_stocks.get('data', []))}")
    if hot_stocks.get('data'):
        print(f"First hot stock: {hot_stocks['data'][0]}")

if __name__ == "__main__":
    asyncio.run(main())
