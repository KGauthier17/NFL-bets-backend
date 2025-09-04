# NFL Bets Backend

A comprehensive backend system for analyzing NFL player prop bets using statistical modeling and machine learning. This system calculates the probability of player props hitting based on historical performance data and identifies high-value betting opportunities.

## What This Backend Does

### 📊 Statistical Analysis
- **Player Performance Tracking**: Monitors and analyzes historical player statistics across multiple categories (rushing, passing, receiving)
- **Rolling Statistics**: Calculates weighted averages and standard deviations using recent game performance data
- **Position-Specific Modeling**: Tailors probability calculations based on player positions (QB, RB, WR, TE)

### 🎯 Probability Calculations
- **Multiple Distribution Models**: Uses Normal, Poisson, and Negative Binomial distributions depending on stat type
- **Combined Statistics**: Handles complex props like total touchdowns, rush+receiving yards
- **Binary Props**: Calculates anytime touchdown probabilities and other yes/no propositions

### 🔍 Prop Bet Analysis
- **Real-Time Processing**: Fetches daily player props from sportsbooks
- **Value Assessment**: Compares calculated probabilities against betting odds to identify profitable opportunities
- **Fuzzy Player Matching**: Intelligently matches player names across different data sources

### 🏈 Supported Prop Types
- **Rushing**: Yards, attempts, touchdowns, longest rush
- **Passing**: Yards, attempts, completions, touchdowns, interceptions
- **Receiving**: Yards, receptions, touchdowns, longest reception
- **Combined**: Rush+receiving yards, total touchdowns, anytime TD

## Key Features

- **AWS Integration**: Built on DynamoDB for scalable data storage
- **Smart Caching**: Efficient player name-to-ID mapping with fuzzy search
- **Statistical Rigor**: Multiple probability models for different stat distributions
- **Position Awareness**: QB-specific logic for passing vs rushing TDs
- **Daily Automation**: Designed for daily prop analysis workflows

## Use Cases

1. **Identify Value Bets**: Find props where your calculated probability exceeds implied odds probability
2. **Risk Assessment**: Understand the true likelihood of props hitting based on player history
3. **Portfolio Analysis**: Analyze multiple props across different players and games
4. **Model Validation**: Compare predicted vs actual outcomes to refine probability models

## Data Sources

- Player rolling statistics (weighted by recent performance)
- Daily sportsbook prop lines and odds
- Historical player performance data
- Position and team information

This backend serves as the analytical engine for making data-driven NFL prop betting decisions, moving beyond gut feelings to statistical probability and rigorous analysis.