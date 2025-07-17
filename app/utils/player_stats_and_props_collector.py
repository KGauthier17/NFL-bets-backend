import boto3
import requests
import os
from decimal import Decimal
from botocore.exceptions import ClientError
import time
import re
from typing import Dict, List
from dotenv import load_dotenv
import datetime

load_dotenv()

def load_popular_players(filepath="app/config/popular_offensive_players.txt"):
    with open(filepath, "r") as f:
        return set(line.strip().lower() for line in f if line.strip())

class PlayerStatsAndPropsCollector:
    def __init__(self, aws_region: str = 'us-west-2'):
        self.aws_region = aws_region
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.popular_offensive_players_set = load_popular_players()
        self.player_data_api_key = os.getenv('SPORTSDATA_API_KEY')
        self.player_prop_api_key = os.getenv('SPORTSPROPS_API_KEY')
        self.player_data_base_url = "https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek"
        self.player_prop_base_url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events"

    def get_table(self, table_name: str):
        return self.dynamodb.Table(table_name)

    def put_item(self, table_name: str, item: Dict):
        table = self.get_table(table_name)
        with table.batch_writer() as batch:
            batch.put_item(Item=item)

    def get_player_stats(self, year: int, week: int) -> List[Dict]:
        url = f"{self.player_data_base_url}/{year}/{week}?key={self.player_data_api_key}"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for Year {year}, Week {week}: {e}")
            return []

    def get_event_ids(self) -> List[str]:
        table = self.dynamodb.Table('nfl_events')
        try:
            response = table.scan()
            items = response.get('Items', [])
            event_ids = []
            for item in items:
                commence_time = item.get('commence_time')
                event_id = item.get('id')
                if not commence_time or not event_id:
                    continue
                event_date = datetime.datetime.fromisoformat(commence_time).date()
                today = datetime.datetime.now().date()
                if today == event_date:
                    event_ids.append(event_id)
            return event_ids
        except ClientError as e:
            print(f"Error scanning nfl_events table: {e}")
            return []

    def get_player_props(self, event_id: str):
        player_prop_keys = [
            "player_last_td", "player_anytime_td", "player_1st_td", "player_tds_over", "player_rush_yds",
            "player_rush_tds", "player_rush_reception_yds", "player_rush_reception_tds",
            "player_rush_longest", "player_rush_attempts", "player_pass_tds", "player_assists",
            "player_defensive_interceptions",
            "player_pass_attempts", "player_pass_completions", "player_pass_interceptions",
            "player_pass_longest_completion", "player_pass_rush_reception_tds",
            "player_pass_rush_reception_yds", "player_pass_yds", "player_pass_yds_q1",
            "player_pats", "player_receptions", "player_reception_longest",
            "player_reception_tds", "player_reception_yds"
        ]
        markets = "%2C".join(player_prop_keys)
        url = (
            f"{self.player_prop_base_url}/{event_id}/odds?"
            f"apiKey={self.player_prop_api_key}&regions=us&markets={markets}"
            f"&dateFormat=iso&oddsFormat=american&includeLinks=true&includeSids=true&includeBetLimits=true"
        )
        print(url)
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching player props for Event {event_id}: {e}")
            return []

    def should_store_player(self, player_data: Dict) -> bool:
        position_category = player_data.get('PositionCategory', '')
        activated = player_data.get('Activated', 0)
        is_offensive = position_category == 'OFF'
        is_activated = activated == 1
        return is_offensive and is_activated

    def extract_prop_betting_stats(self, player_data: Dict) -> Dict:
        prop_stats = {
            'player_id': player_data.get('PlayerID'),
            'name': player_data.get('Name'),
            'position': player_data.get('Position'),
            'position_category': player_data.get('PositionCategory'),
            'team': player_data.get('Team'),
            'opponent': player_data.get('Opponent'),
            'home_or_away': player_data.get('HomeOrAway'),
            'game_date': player_data.get('GameDate'),
            'activated': player_data.get('Activated', 0),
            'played': player_data.get('Played', 0),
            'passing_yards': player_data.get('PassingYards', 0),
            'passing_touchdowns': player_data.get('PassingTouchdowns', 0),
            'passing_interceptions': player_data.get('Interceptions', 0),
            'passing_attempts': player_data.get('PassingAttempts', 0),
            'passing_completions': player_data.get('PassingCompletions', 0),
            'passing_completion_percentage': player_data.get('PassingCompletionPercentage', 0),
            'passing_rating': player_data.get('PassingRating', 0),
            'rushing_yards': player_data.get('RushingYards', 0),
            'rushing_touchdowns': player_data.get('RushingTouchdowns', 0),
            'rushing_attempts': player_data.get('RushingAttempts', 0),
            'rushing_average': player_data.get('RushingYardsPerAttempt', 0),
            'rushing_long': player_data.get('RushingLong', 0),
            'receiving_yards': player_data.get('ReceivingYards', 0),
            'receiving_touchdowns': player_data.get('ReceivingTouchdowns', 0),
            'receptions': player_data.get('Receptions', 0),
            'targets': player_data.get('ReceivingTargets', 0),
            'receiving_average': player_data.get('ReceivingYardsPerReception', 0),
            'receiving_long': player_data.get('ReceivingLong', 0),
            'fumbles': player_data.get('Fumbles', 0),
            'fumbles_lost': player_data.get('FumblesLost', 0),
            'two_point_conversions': player_data.get('TwoPointConversions', 0),
        }
        for key, value in prop_stats.items():
            if value is None:
                prop_stats[key] = 0
            elif isinstance(value, float):
                prop_stats[key] = Decimal(str(value))
            elif isinstance(value, int):
                prop_stats[key] = value
            else:
                prop_stats[key] = str(value) if value is not None else ""
        return prop_stats

    def process_nfl_season_data(self, year: int = 2025, week: int = 1):
        total_players_processed = 0
        total_players_filtered = 0
        player_stats = self.get_player_stats(year, week)
        if not player_stats:
            print(f"No data found for Week {week}")
            return
        processed_players = 0
        filtered_players = 0
        items = []
        for player_data in player_stats:
            player_id = player_data.get('PlayerID')
            player_name = player_data.get('Name')
            if player_name.lower() not in self.popular_offensive_players_set:
                filtered_players += 1
                continue
            if not player_id or not player_name:
                continue
            if not self.should_store_player(player_data):
                filtered_players += 1
                continue
            prop_stats = self.extract_prop_betting_stats(player_data)
            prop_stats['player_id'] = str(player_id)
            prop_stats['season_week'] = f"{year}_week_{week}"
            prop_stats['year'] = year
            prop_stats['week'] = week
            items.append(prop_stats)
            processed_players += 1
        table = self.dynamodb.Table('nfl_player_stats')
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        total_players_processed += processed_players
        total_players_filtered += filtered_players
        time.sleep(1)

    def get_player_season_data(self, player_name: str, player_id: str, year: int) -> List[Dict]:
        def sanitize_player_name(name: str) -> str:
            if not name:
                return "unknown_player"
            sanitized = name.lower().replace(' ', '_')
            sanitized = re.sub(r'[^a-z0-9_\-.]', '', sanitized)
            sanitized = sanitized.strip('_')
            if not sanitized:
                sanitized = "player"
            return sanitized
        sanitized_name = sanitize_player_name(player_name)
        table_name = f"{sanitized_name}_{player_id}"
        try:
            table = self.dynamodb.Table(table_name)
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('year').eq(year)
            )
            return response.get('Items', [])
        except ClientError as e:
            print(f"Error retrieving data for {player_name} (ID: {player_id}): {e}")
            return []

    @staticmethod
    def get_week_of_season():
        season_start = datetime.datetime(2025, 9, 5, 0, 0, 0, tzinfo=datetime.timezone.utc)
        now = datetime.datetime.now(datetime.timezone.utc)
        if now < season_start:
            return None
        days_since_start = (now - season_start).days
        week = days_since_start // 7 + 1
        if week < 1 or week > 18:
            return None
        return season_start.year, week