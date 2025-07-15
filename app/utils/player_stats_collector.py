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
import time

load_dotenv()

def load_popular_players(filepath="app/config/popular_offensive_players.txt"):
    with open(filepath, "r") as f:
        return set(line.strip().lower() for line in f if line.strip())
    
popular_offensive_players_set = load_popular_players()

def process_nfl_season_data(year: int = 2024, week: int = 1, aws_region: str = 'us-west-2'):
    
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    try:
        if aws_access_key_id and aws_secret_access_key:
            dynamodb = boto3.resource(
                'dynamodb',
                region_name=aws_region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        else:
            dynamodb = boto3.resource('dynamodb', region_name=aws_region)
            
    except Exception as e:
        print(f"Error initializing AWS resources: {e}")
        raise
    
    api_key = os.getenv('SPORTSDATA_API_KEY')
    if not api_key:
        print("Error: SPORTSDATA_API_KEY environment variable is not set.")
        return
    base_url = "https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek"

    def get_player_stats(year: int, week: int) -> List[Dict]:
        """Fetch player statistics from the API for a specific week"""
        url = f"{base_url}/{year}/{week}?key={api_key}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for Year {year}, Week {week}: {e}")
            return []

    def should_store_player(player_data: Dict) -> bool:
        """
        Filter players based on criteria:
        - PositionCategory must be "OFF" (offensive players only)
        - Activated must be 1
        - Played must be 1
        """
        position_category = player_data.get('PositionCategory', '')
        activated = player_data.get('Activated', 0)
        
        is_offensive = position_category == 'OFF'
        is_activated = activated == 1
        
        if not is_offensive:
            return False
        if not is_activated:
            return False
            
        return True
    
    def extract_prop_betting_stats(player_data: Dict) -> Dict:
        """Extract relevant statistics for prop betting"""
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
    
    total_players_processed = 0
    total_players_filtered = 0
        
    player_stats = get_player_stats(year, week)
    
    if not player_stats:
        print(f"No data found for Week {week}")
        return
    
    processed_players = 0
    filtered_players = 0
    
    items = []
    for player_data in player_stats:
        player_id = player_data.get('PlayerID')
        player_name = player_data.get('Name')
        
        if player_name.lower() not in popular_offensive_players_set:
            filtered_players += 1
            continue
        
        if not player_id or not player_name:
            continue
        
        if not should_store_player(player_data):
            filtered_players += 1
            continue

        prop_stats = extract_prop_betting_stats(player_data)
        prop_stats['player_id'] = str(player_id)
        prop_stats['season_week'] = f"{year}_week_{week}"
        prop_stats['year'] = year
        prop_stats['week'] = week
        items.append(prop_stats)
        processed_players += 1

    table = dynamodb.Table('nfl_player_stats')
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
            
    print(f"Week {week}: Processed {processed_players} players, Filtered out {filtered_players} players")
    total_players_processed += processed_players
    total_players_filtered += filtered_players
    
    time.sleep(1)


def get_player_season_data(player_name: str, player_id: str, year: int, aws_region: str = 'us-west-2') -> List[Dict]:
    """
    Retrieve all season data for a specific player
    
    Args:
        player_name (str): Player name
        player_id (str): Player ID
        year (int): Season year
        aws_region (str): AWS region for DynamoDB
        
    Returns:
        List[Dict]: List of weekly statistics
    """
    def sanitize_player_name(name: str) -> str:
        """Same sanitization logic as above"""
        if not name:
            return "unknown_player"
        
        sanitized = name.lower().replace(' ', '_')
        sanitized = re.sub(r'[^a-z0-9_\-.]', '', sanitized)
        sanitized = sanitized.strip('_')
        
        if not sanitized:
            sanitized = "player"
        
        return sanitized
    
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    sanitized_name = sanitize_player_name(player_name)
    table_name = f"{sanitized_name}_{player_id}"
    
    try:
        table = dynamodb.Table(table_name)
        
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('year').eq(year)
        )
        
        return response.get('Items', [])
        
    except ClientError as e:
        print(f"Error retrieving data for {player_name} (ID: {player_id}): {e}")
        return []

def get_week_of_season():
    """
    Get the current week of the NFL season based on the official start date.
    Returns:
        (int, int): (current year, current week number) or None if not in season
    """
    season_start = datetime.datetime(2025, 9, 30, 0, 15, 0, tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)

    if now < season_start:
        return None

    days_since_start = (now - season_start).days
    week = days_since_start // 7 + 1

    if week < 1 or week > 18:
        return None

    return season_start.year, week
