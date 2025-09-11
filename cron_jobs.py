import os
import sys
from datetime import datetime, timezone, date
from app.services import player_stats_and_props_collector as pspc
from app.services.rolling_stats_calculator import RollingStatsCalculator
from app.services.probability_calculator import ProbabilityCalculator

def run_daily_jobs():
    """Execute all daily data collection and processing jobs"""
    try:
        print(f"🚀 Starting daily jobs at {datetime.now(timezone.utc)}")
        
        # Initialize services
        data_collector = pspc.PlayerStatsAndPropsCollector()
        calculator = RollingStatsCalculator()
        prob_calculator = ProbabilityCalculator()
        
        # Check if NFL season is active
        week_info = data_collector.get_week_of_season()
        if week_info is None:
            print("ℹ️ NFL season is not currently active. No data to process.")
            return True
        
        year, week = week_info
        print(f"📅 Processing NFL season {year}, week {week}")
        
        # Only process CURRENT week - not all previous weeks
        print(f"📊 Processing current week {week}...")
        data_collector.process_nfl_season_data(year, week)
        
        # Check if we already updated props today to avoid duplicate calls
        today = date.today()
        print(f"🎯 Checking if player props need updating for {today}...")
        
        data_collector.update_today_player_props()
        
        # Update rolling stats (this should be efficient and not call external APIs)
        print("📈 Calculating rolling stats...")
        calculator.update_all_rolling_stats()
        
        # NEW: Cache probabilities for today's props
        print("🔄 Caching probabilities for today's props...")
        prob_calculator.cache_todays_probabilities()
        
        print("✅ All daily jobs completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error running daily jobs: {e}")
        return False

if __name__ == "__main__":
    success = run_daily_jobs()
    sys.exit(0 if success else 1)