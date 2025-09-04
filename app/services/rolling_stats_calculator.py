import boto3
import numpy as np
from decimal import Decimal
from typing import Dict, List, Optional
from botocore.exceptions import ClientError
import statistics
from collections import defaultdict

class RollingStatsCalculator:
    def __init__(self, aws_region: str = 'us-west-2'):
        self.aws_region = aws_region
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        
        # Define statistical columns to track
        self.stat_columns = [
            'passing_yards', 'passing_touchdowns', 'passing_interceptions',
            'passing_attempts', 'passing_completions', 'rushing_yards',
            'rushing_touchdowns', 'rushing_attempts', 'rushing_long',
            'receiving_yards', 'receiving_touchdowns', 'receptions', 
            'targets', 'receiving_long'
        ]
        
        # Exponential decay weights (more recent weeks weighted higher)
        self.max_weeks = 18
        self.decay_factor = 0.9  # Adjust this to control how much recent weeks are favored
    
    def get_exponential_weights(self, num_weeks: int) -> List[float]:
        """Generate exponential decay weights with most recent weeks weighted highest"""
        weights = []
        for i in range(num_weeks):
            # Weight decreases exponentially for older weeks
            weight = self.decay_factor ** i
            weights.append(weight)
        
        # Reverse so most recent week has highest weight
        weights.reverse()
        
        # Normalize weights to sum to 1
        total_weight = sum(weights)
        return [w / total_weight for w in weights]
    
    def calculate_weighted_mean(self, values: List[float], weights: List[float]) -> float:
        """Calculate weighted mean"""
        if not values or not weights or len(values) != len(weights):
            return 0.0
        return sum(v * w for v, w in zip(values, weights))
    
    def calculate_weighted_std(self, values: List[float], weights: List[float], weighted_mean: float) -> float:
        """Calculate weighted standard deviation"""
        if not values or not weights or len(values) != len(weights) or len(values) < 2:
            return 0.0
        
        variance = sum(w * (v - weighted_mean) ** 2 for v, w in zip(values, weights))
        return variance ** 0.5
    
    def calculate_lambda_poisson(self, values: List[float], weights: List[float]) -> float:
        """Calculate lambda parameter for Poisson distribution (weighted mean)"""
        return self.calculate_weighted_mean(values, weights)
    
    def get_player_historical_stats(self, player_id: str) -> List[Dict]:
        """Fetch all historical stats for a player, ordered by week"""
        table = self.dynamodb.Table('nfl_player_stats')
        try:
            response = table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('player_id').eq(player_id),
                ScanIndexForward=True  # Sort by sort key (season_week) in ascending order
            )
            return sorted(response.get('Items', []), key=lambda x: (x.get('year', 0), x.get('week', 0)))
        except ClientError as e:
            print(f"Error querying player stats for {player_id}: {e}")
            return []
    
    def calculate_rolling_stats_for_player(self, player_id: str, player_name: str) -> Optional[Dict]:
        """Calculate rolling statistics for a single player"""
        historical_stats = self.get_player_historical_stats(player_id)
        
        if not historical_stats:
            return None
        
        # Group stats by statistic type
        stat_values = defaultdict(list)
        
        # Extract values for each stat column
        for game_stat in historical_stats:
            for stat_col in self.stat_columns:
                value = game_stat.get(stat_col, 0)
                if isinstance(value, Decimal):
                    value = float(value)
                stat_values[stat_col].append(float(value))
        
        # Calculate rolling stats for each statistic
        rolling_stats = {
            'player_id': player_id,
            'player_name': player_name,
            'total_games': len(historical_stats),
            'last_updated': historical_stats[-1].get('game_date', '') if historical_stats else '',
            'team': historical_stats[-1].get('team', '') if historical_stats else '',
            'position': historical_stats[-1].get('position', '') if historical_stats else ''
        }
        
        for stat_col in self.stat_columns:
            values = stat_values[stat_col]
            if not values:
                continue
                
            num_weeks = len(values)
            weights = self.get_exponential_weights(num_weeks)
            
            # Calculate weighted statistics
            weighted_mean = self.calculate_weighted_mean(values, weights)
            weighted_std = self.calculate_weighted_std(values, weights, weighted_mean)
            lambda_param = self.calculate_lambda_poisson(values, weights)
            
            # Also calculate simple averages for comparison
            simple_mean = statistics.mean(values) if values else 0
            simple_std = statistics.stdev(values) if len(values) > 1 else 0
            
            # Store all statistical measures
            rolling_stats.update({
                f'{stat_col}_weighted_mean': Decimal(str(round(weighted_mean, 3))),
                f'{stat_col}_weighted_std': Decimal(str(round(weighted_std, 3))),
                f'{stat_col}_lambda': Decimal(str(round(lambda_param, 3))),
                f'{stat_col}_simple_mean': Decimal(str(round(simple_mean, 3))),
                f'{stat_col}_simple_std': Decimal(str(round(simple_std, 3))),
                f'{stat_col}_sample_size': num_weeks
            })
        
        return rolling_stats
    
    def update_all_rolling_stats(self):
        """Update rolling statistics for all players in the database"""
        # Get all unique players from nfl_player_stats
        stats_table = self.dynamodb.Table('nfl_player_stats')
        rolling_table = self.dynamodb.Table('nfl_player_rolling_stats')
        
        try:
            # Scan to get all unique player IDs
            response = stats_table.scan(
                ProjectionExpression='player_id, #name',
                ExpressionAttributeNames={'#name': 'name'}
            )
            
            unique_players = {}
            for item in response['Items']:
                player_id = item.get('player_id')
                player_name = item.get('name')
                if player_id and player_name:
                    unique_players[player_id] = player_name
            
            # Process pagination if needed
            while 'LastEvaluatedKey' in response:
                response = stats_table.scan(
                    ProjectionExpression='player_id, #name',
                    ExpressionAttributeNames={'#name': 'name'},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response['Items']:
                    player_id = item.get('player_id')
                    player_name = item.get('name')
                    if player_id and player_name:
                        unique_players[player_id] = player_name
            
            print(f"Processing rolling stats for {len(unique_players)} players...")
            
            # Calculate and store rolling stats for each player
            with rolling_table.batch_writer() as batch:
                for player_id, player_name in unique_players.items():
                    rolling_stats = self.calculate_rolling_stats_for_player(player_id, player_name)
                    if rolling_stats:
                        batch.put_item(Item=rolling_stats)
                        print(f"Updated rolling stats for {player_name}")
        
        except ClientError as e:
            print(f"Error updating rolling stats: {e}")