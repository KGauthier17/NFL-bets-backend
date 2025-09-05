import boto3
import numpy as np
from scipy import stats
from decimal import Decimal
from typing import Dict, Optional, List
from botocore.exceptions import ClientError
import datetime
from rapidfuzz import process, fuzz

class ProbabilityCalculator:
    def __init__(self, aws_region: str = 'us-west-2'):
        self.aws_region = aws_region
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        
        # Cache for player name to ID mapping
        self.player_name_cache = None
        
        # Mapping of prop market keys to our stat column names
        self.prop_to_stat_mapping = {
            'player_rush_yds': 'rushing_yards',
            'player_rush_tds': 'rushing_touchdowns', 
            'player_rush_attempts': 'rushing_attempts',
            'player_rush_longest': 'rushing_long',
            'player_pass_yds': 'passing_yards',
            'player_pass_tds': 'passing_touchdowns',
            'player_pass_attempts': 'passing_attempts',
            'player_pass_completions': 'passing_completions',
            'player_reception_yds': 'receiving_yards',
            'player_reception_tds': 'receiving_touchdowns',
            'player_receptions': 'receptions',
            'player_reception_longest': 'receiving_long',
            'player_rush_reception_yds': 'combined_rush_receiving_yards',  # Special handling
            'player_rush_reception_tds': 'combined_rush_receiving_tds',  # Special handling
            'player_tds': 'combined_touchdowns',  # Special handling
            'player_anytime_td': 'anytime_touchdown',  # Special handling - binary prop# Special handling - binary prop
        }
    
    def load_player_name_cache(self) -> Dict[str, str]:
        """Load all player names and IDs from rolling stats table into cache"""
        if self.player_name_cache is not None:
            return self.player_name_cache
            
        table = self.dynamodb.Table('nfl_player_rolling_stats')
        self.player_name_cache = {}
        
        try:
            response = table.scan(
                ProjectionExpression='player_id, player_name'
            )
            
            for item in response.get('Items', []):
                player_id = item.get('player_id')
                player_name = item.get('player_name', '').lower()
                if player_id and player_name:
                    self.player_name_cache[player_name] = player_id
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    ProjectionExpression='player_id, player_name',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response.get('Items', []):
                    player_id = item.get('player_id')
                    player_name = item.get('player_name', '').lower()
                    if player_id and player_name:
                        self.player_name_cache[player_name] = player_id
        
        except ClientError as e:
            print(f"Error loading player name cache: {e}")
            self.player_name_cache = {}
        
        return self.player_name_cache
    
    def find_player_id_by_name_fuzzy(self, player_name: str, threshold: int = 85) -> Optional[str]:
        """Find player ID by name using fuzzy matching"""
        name_cache = self.load_player_name_cache()
        
        if not name_cache:
            return None
        
        # First try exact match
        clean_name = player_name.lower().strip()
        if clean_name in name_cache:
            return name_cache[clean_name]
        
        # Then try fuzzy matching
        try:
            available_names = list(name_cache.keys())
            match, score, _ = process.extractOne(
                clean_name,
                available_names,
                scorer=fuzz.token_sort_ratio
            )
            
            if score >= threshold:
                return name_cache[match]
            else:
                print(f"No good match found for '{player_name}' (best match: '{match}' with score {score})")
                return None
                
        except Exception as e:
            print(f"Error in fuzzy matching for {player_name}: {e}")
            return None
    
    def get_player_rolling_stats(self, player_id: str) -> Optional[Dict]:
        """Get rolling statistics for a player"""
        table = self.dynamodb.Table('nfl_player_rolling_stats')
        try:
            response = table.get_item(Key={'player_id': player_id})
            return response.get('Item')
        except ClientError as e:
            print(f"Error fetching rolling stats for player {player_id}: {e}")
            return None

    def get_todays_player_props(self) -> List[Dict]:
        """Get player props for today, or fallback to most recent props if none available"""
        table = self.dynamodb.Table('nfl_player_props')
        today = datetime.datetime.now().date().isoformat()
        
        try:
            response = table.scan()
            items = response.get('Items', [])
            
            # Handle pagination to get all items
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))
            
            # First, try to find props for today
            todays_props = []
            for item in items:
                prop_date = item.get('date', '')
                if prop_date.startswith(today):
                    todays_props.append(item)
            
            if todays_props:
                print(f"Found {len(todays_props)} props for today ({today})")
                return todays_props
            
            # If no props for today, get the most recent props
            print(f"No props found for today ({today}), looking for most recent props...")
            
            # Group items by date and find the most recent date
            props_by_date = {}
            for item in items:
                prop_date = item.get('date', '')
                if prop_date:  # Only consider items with valid dates
                    date_key = prop_date.split('T')[0]  # Extract date part (YYYY-MM-DD)
                    if date_key not in props_by_date:
                        props_by_date[date_key] = []
                    props_by_date[date_key].append(item)
            
            if not props_by_date:
                print("No props found in database")
                return []
            
            # Find the most recent date
            most_recent_date = max(props_by_date.keys())
            most_recent_props = props_by_date[most_recent_date]
            
            print(f"Using most recent props from {most_recent_date} ({len(most_recent_props)} props)")
            return most_recent_props
            
        except ClientError as e:
            print(f"Error scanning player props: {e}")
            return []

    def calculate_normal_probability(self, mean: float, std: float, prop_line: float, over: bool = True) -> float:
        """Calculate probability using normal distribution"""
        if std <= 0:
            return 0.5  # Return neutral probability if no variance
        
        if over:
            # P(X > prop_line)
            return 1 - stats.norm.cdf(prop_line, mean, std)
        else:
            # P(X < prop_line)
            return stats.norm.cdf(prop_line, mean, std)
    
    def calculate_poisson_probability(self, lambda_param: float, prop_line: float, over: bool = True) -> float:
        """Calculate probability using Poisson distribution"""
        if lambda_param <= 0:
            return 0.0
        
        if over:
            # P(X > prop_line) = 1 - P(X <= prop_line)
            return 1 - stats.poisson.cdf(int(prop_line), lambda_param)
        else:
            # P(X < prop_line) = P(X <= prop_line - 1)
            return stats.poisson.cdf(int(prop_line - 1), lambda_param) if prop_line > 0 else 0
    
    def calculate_negative_binomial_probability(self, mean: float, variance: float, prop_line: float, over: bool = True) -> float:
        """Calculate probability using negative binomial distribution"""
        if mean <= 0 or variance <= mean:
            return self.calculate_poisson_probability(mean, prop_line, over)
        
        # Calculate parameters for negative binomial
        p = mean / variance
        r = (mean * p) / (1 - p)
        
        if r <= 0 or p <= 0 or p >= 1:
            return self.calculate_poisson_probability(mean, prop_line, over)
        
        if over:
            return 1 - stats.nbinom.cdf(int(prop_line), r, p)
        else:
            return stats.nbinom.cdf(int(prop_line - 1), r, p) if prop_line > 0 else 0

    def calculate_combined_touchdowns_probabilities(self, player_id: str, prop_line: float) -> Dict:
        """Calculate probabilities for total touchdowns (rushing + receiving)"""
        rolling_stats = self.get_player_rolling_stats(player_id)
        
        if not rolling_stats:
            return {'error': 'Player rolling stats not found'}
        
        # Get TD stats for different types
        rushing_tds_mean = float(rolling_stats.get('rushing_touchdowns_weighted_mean', 0))
        receiving_tds_mean = float(rolling_stats.get('receiving_touchdowns_weighted_mean', 0))
        
        # For ALL positions (including QBs), only count touchdowns they score themselves
        # This is rushing TDs + receiving TDs (NOT passing TDs)
        total_td_mean = rushing_tds_mean + receiving_tds_mean
    
        # Get sample sizes for weighting
        rushing_sample = int(rolling_stats.get('rushing_touchdowns_sample_size', 0))
        receiving_sample = int(rolling_stats.get('receiving_touchdowns_sample_size', 0))
        sample_size = max(rushing_sample, receiving_sample)
        
        # Use Poisson for total TDs (sum of Poisson variables is Poisson)
        over_prob = self.calculate_poisson_probability(total_td_mean, prop_line, True)
        under_prob = self.calculate_poisson_probability(total_td_mean, prop_line, False)
        
        return {
            'over_probability': round(over_prob, 4),
            'under_probability': round(under_prob, 4),
            'sample_size': sample_size,
            'weighted_mean': round(total_td_mean, 2),
            'distribution_used': 'poisson_combined'
        }
    
    def calculate_combined_rush_receiving_yards_probabilities(self, player_id: str, prop_line: float) -> Dict:
        """Calculate probabilities for combined rushing + receiving yards"""
        rolling_stats = self.get_player_rolling_stats(player_id)
        
        if not rolling_stats:
            return {'error': 'Player rolling stats not found'}
        
        # Get yards stats
        rushing_yards_mean = float(rolling_stats.get('rushing_yards_weighted_mean', 0))
        receiving_yards_mean = float(rolling_stats.get('receiving_yards_weighted_mean', 0))
        
        rushing_yards_std = float(rolling_stats.get('rushing_yards_weighted_std', 0))
        receiving_yards_std = float(rolling_stats.get('receiving_yards_weighted_std', 0))
        
        # Combined stats (assuming independence)
        total_yards_mean = rushing_yards_mean + receiving_yards_mean
        total_yards_std = (rushing_yards_std ** 2 + receiving_yards_std ** 2) ** 0.5
        
        # Get sample sizes
        rushing_sample = int(rolling_stats.get('rushing_yards_sample_size', 0))
        receiving_sample = int(rolling_stats.get('receiving_yards_sample_size', 0))
        sample_size = max(rushing_sample, receiving_sample)
        
        # Use normal distribution for combined yards
        over_prob = self.calculate_normal_probability(total_yards_mean, total_yards_std, prop_line, True)
        under_prob = self.calculate_normal_probability(total_yards_mean, total_yards_std, prop_line, False)
        
        return {
            'over_probability': round(over_prob, 4),
            'under_probability': round(under_prob, 4),
            'sample_size': sample_size,
            'weighted_mean': round(total_yards_mean, 2),
            'distribution_used': 'normal_combined'
        }

    def get_best_probability_improved(self, normal_prob: float, poisson_prob: float, neg_bin_prob: float, 
                                    stat_type: str, weighted_mean: float, weighted_std: float, sample_size: int) -> float:
        """Choose the best probability based on statistic type and data characteristics"""
        
        if sample_size < 3:
            return 0.5  # Not enough data
        
        # Calculate coefficient of variation (std/mean) to assess distribution shape
        cv = weighted_std / weighted_mean if weighted_mean > 0 else float('inf')
        
        # Define stat categories and their typical characteristics
        count_stats = ['rushing_attempts', 'passing_attempts', 'passing_completions', 'receptions', 'targets']
        discrete_bounded_stats = ['passing_touchdowns', 'rushing_touchdowns', 'receiving_touchdowns']
        continuous_stats = ['passing_yards', 'rushing_yards', 'receiving_yards', 'rushing_long', 'receiving_long']
        
        # Distribution selection logic
        if stat_type in count_stats:
            # For attempt/count stats - usually well-modeled by Poisson or Negative Binomial
            if cv <= 1.2:  # Low overdispersion
                return round(poisson_prob, 4)
            else:  # High overdispersion
                return round(neg_bin_prob, 4)
                
        elif stat_type in discrete_bounded_stats:
            # For touchdown/interception stats - typically low counts with overdispersion
            if weighted_mean < 0.5:  # Very low rate events
                return round(poisson_prob, 4)
            else:
                return round(neg_bin_prob, 4)
                
        elif stat_type in continuous_stats:
            # For yardage and longest stats - can be modeled various ways depending on characteristics
            if stat_type in ['rushing_long', 'receiving_long']:
                # Longest plays tend to have high variance - prefer negative binomial
                return round(neg_bin_prob, 4)
            elif sample_size >= 10 and cv <= 0.8:  # Low variability, sufficient data
                return round(normal_prob, 4)
            elif cv > 1.5:  # High overdispersion
                return round(neg_bin_prob, 4)
            else:  # Moderate overdispersion
                if sample_size >= 8:
                    return round(neg_bin_prob, 4)
                else:
                    # Blend approaches for smaller samples
                    return round((normal_prob + neg_bin_prob) / 2, 4)
        
        # Default fallback
        if sample_size >= 10:
            return round(neg_bin_prob, 4)
        else:
            return round((normal_prob + poisson_prob + neg_bin_prob) / 3, 4)

    def get_distribution_name(self, stat_type: str, weighted_mean: float, weighted_std: float, sample_size: int) -> str:
        """Return which distribution was selected for transparency"""
        if sample_size < 3:
            return "insufficient_data"
        
        cv = weighted_std / weighted_mean if weighted_mean > 0 else float('inf')
        count_stats = ['rushing_attempts', 'passing_attempts', 'passing_completions', 'receptions', 'targets']
        discrete_bounded_stats = ['passing_touchdowns', 'rushing_touchdowns', 'receiving_touchdowns', 'passing_interceptions']
        continuous_stats = ['passing_yards', 'rushing_yards', 'receiving_yards', 'rushing_long', 'receiving_long']
        
        if stat_type in count_stats:
            return "poisson" if cv <= 1.2 else "negative_binomial"
        elif stat_type in discrete_bounded_stats:
            return "poisson" if weighted_mean < 0.5 else "negative_binomial"
        elif stat_type in continuous_stats:
            if stat_type in ['rushing_long', 'receiving_long']:
                return "negative_binomial"
            elif sample_size >= 10 and cv <= 0.8:
                return "normal"
            elif cv > 1.5:
                return "negative_binomial"
            else:
                return "negative_binomial" if sample_size >= 8 else "blended"
        
        return "negative_binomial" if sample_size >= 10 else "averaged"
    
    def get_all_todays_probabilities(self) -> Dict:
        """Get probabilities for all players with props today, or most recent props if none today"""
        todays_props = self.get_todays_player_props()
        probabilities = {}
        
        if not todays_props:
            print("No props available in database")
            return probabilities
        
        print(f"Processing {len(todays_props)} prop items")
        
        for prop_item in todays_props:
            player_name = prop_item.get('player_name', '')
            markets = prop_item.get('markets', [])
            
            if not player_name or not markets:
                continue
            
            # Find player ID using fuzzy matching
            player_id = self.find_player_id_by_name_fuzzy(player_name)
            if not player_id:
                print(f"Could not find player ID for '{player_name}' after fuzzy matching")
                continue
            
            print(f"Found player ID {player_id} for '{player_name}'")
            player_probs = {}
            
            for market in markets:
                market_key = market.get('market_key', '')
                point = market.get('point')
                
                if not market_key:
                    continue
                
                # Handle binary props (anytime TD, first TD, last TD)
                if market_key in ['player_anytime_td', 'player_1st_td', 'player_last_td']:
                    if market_key == 'player_anytime_td':
                        prob_result = self.calculate_anytime_td_probability(player_id)
                    else:
                        # For first/last TD, we can use a similar approach but with lower probabilities
                        # For now, skip these as they require more complex modeling
                        print(f"Skipping {market_key} - requires specialized modeling")
                        continue
                    
                    if 'error' not in prob_result:
                        # For binary props, use Yes/No instead of Over/Under
                        yes_name = f"{market_key}_yes"
                        no_name = f"{market_key}_no"
                        
                        player_probs[yes_name] = prob_result['yes_probability']
                        player_probs[no_name] = prob_result['no_probability']
                    else:
                        print(f"Error calculating probabilities for {player_name} {market_key}: {prob_result['error']}")
                    continue
                
                # For over/under props, we need a point value
                if point is None:
                    print(f"Skipping {market_key} for {player_name} - no point value")
                    continue
                
                # Convert point to float if it's a Decimal
                if isinstance(point, Decimal):
                    point = float(point)
                
                # Handle special combined stats
                if market_key == 'player_tds':
                    prob_result = self.calculate_combined_touchdowns_probabilities(player_id, point)
                elif market_key == 'player_rush_reception_yds':
                    prob_result = self.calculate_combined_rush_receiving_yards_probabilities(player_id, point)
                elif market_key == 'player_rush_reception_tds':
                    # This is the same as combined touchdowns for non-QBs
                    prob_result = self.calculate_combined_touchdowns_probabilities(player_id, point)
                else:
                    # Regular single stats
                    stat_type = self.prop_to_stat_mapping.get(market_key)
                    if not stat_type:
                        print(f"No mapping found for market key: {market_key}")
                        continue
                        continue
                    
                    prob_result = self.calculate_prop_probabilities(player_id, stat_type, point)
                
                if 'error' not in prob_result:
                    # Create clean prop names using market_key and point
                    over_name = f"{market_key}_over_{point}"
                    under_name = f"{market_key}_under_{point}"
                    
                    player_probs[over_name] = prob_result['over_probability']
                    player_probs[under_name] = prob_result['under_probability']
                else:
                    print(f"Error calculating probabilities for {player_name} {market_key}: {prob_result['error']}")
            
            if player_probs:
                probabilities[player_name] = player_probs
    
        return probabilities
    
    def calculate_prop_probabilities(self, player_id: str, stat_type: str, prop_line: float) -> Dict:
        """Calculate probabilities for a prop bet using multiple distributions"""
        rolling_stats = self.get_player_rolling_stats(player_id)
        
        if not rolling_stats:
            return {'error': 'Player rolling stats not found'}
        
        # Get the relevant statistics
        weighted_mean_key = f'{stat_type}_weighted_mean'
        weighted_std_key = f'{stat_type}_weighted_std'
        lambda_key = f'{stat_type}_lambda'
        sample_size_key = f'{stat_type}_sample_size'
        
        if weighted_mean_key not in rolling_stats:
            return {'error': f'Stat type {stat_type} not found for player'}
        
        weighted_mean = float(rolling_stats[weighted_mean_key])
        weighted_std = float(rolling_stats[weighted_std_key])
        lambda_param = float(rolling_stats[lambda_key])
        sample_size = int(rolling_stats.get(sample_size_key, 0))
        
        # Calculate variance for negative binomial
        variance = weighted_std ** 2
        
        # Calculate probabilities using different distributions
        normal_over = self.calculate_normal_probability(weighted_mean, weighted_std, prop_line, True)
        poisson_over = self.calculate_poisson_probability(lambda_param, prop_line, True)
        neg_bin_over = self.calculate_negative_binomial_probability(weighted_mean, variance, prop_line, True)
        
        normal_under = self.calculate_normal_probability(weighted_mean, weighted_std, prop_line, False)
        poisson_under = self.calculate_poisson_probability(lambda_param, prop_line, False)
        neg_bin_under = self.calculate_negative_binomial_probability(weighted_mean, variance, prop_line, False)
        
        # Get best probabilities using improved method
        best_over = self.get_best_probability_improved(normal_over, poisson_over, neg_bin_over, 
                                                     stat_type, weighted_mean, weighted_std, sample_size)
        best_under = self.get_best_probability_improved(normal_under, poisson_under, neg_bin_under, 
                                                      stat_type, weighted_mean, weighted_std, sample_size)
        
        return {
            'over_probability': best_over,
            'under_probability': best_under,
            'sample_size': sample_size,
            'weighted_mean': round(weighted_mean, 2),
            'distribution_used': self.get_distribution_name(stat_type, weighted_mean, weighted_std, sample_size)
        }
    
    def calculate_anytime_td_probability(self, player_id: str) -> Dict:
        """Calculate probability of scoring any touchdown (binary outcome)"""
        rolling_stats = self.get_player_rolling_stats(player_id)
        
        if not rolling_stats:
            return {'error': 'Player rolling stats not found'}
        
        # Get TD stats for different types
        rushing_tds_mean = float(rolling_stats.get('rushing_touchdowns_weighted_mean', 0))
        receiving_tds_mean = float(rolling_stats.get('receiving_touchdowns_weighted_mean', 0))
        
        # For ALL positions (including QBs), only count touchdowns they score themselves
        # This is rushing TDs + receiving TDs (NOT passing TDs)
        total_td_mean = rushing_tds_mean + receiving_tds_mean
    
        # Get sample sizes
        rushing_sample = int(rolling_stats.get('rushing_touchdowns_sample_size', 0))
        receiving_sample = int(rolling_stats.get('receiving_touchdowns_sample_size', 0))
        sample_size = max(rushing_sample, receiving_sample)
        
        # For anytime TD, we want P(X >= 1) = 1 - P(X = 0)
        # Using Poisson: P(X = 0) = e^(-λ)
        prob_no_td = self.calculate_poisson_probability(total_td_mean, 0.5, False)  # P(X < 1) = P(X = 0)
        prob_any_td = 1 - prob_no_td
        
        return {
            'yes_probability': round(prob_any_td, 4),
            'no_probability': round(prob_no_td, 4),
            'sample_size': sample_size,
            'weighted_mean': round(total_td_mean, 2),
            'distribution_used': 'poisson_binary'
        }