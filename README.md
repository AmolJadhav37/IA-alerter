This project implements an Alerter, inspired by the VLDB 2006 paper on physical design tuning.
It helps DBAs decide whether running a full tuning tool (Index Advisor) is worth the cost.

The system contains:

Workload Runner → collects baseline and optimized costs using hypothetical indexes

Alerter → analyzes improvement, tuning cost, and space usage to recommend indexes

📌 Features

Supports any SQL workload (filters, aggregates, joins)

Automatically extracts tables and columns from queries

Uses HypoPG to create hypothetical indexes (zero storage)

Computes:
Baseline cost
Optimized cost
Improvement %
Hot columns
Hypothetical index sizes
Alerter performs:
Benefit vs tuning-cost analysis
Space-budget validation
Final YES/NO decision
Outputs recommended CREATE INDEX statements

 Requirements

Python 3.8+
PostgreSQL with HypoPG extension
Python package:
pip install psycopg2
Enable HypoPG:

CREATE EXTENSION IF NOT EXISTS hypopg;

📁 Project Structure
IA-alerter/
│
├── workload_runner.py        # Collects stats using hypothetical indexes
├── alerter.py                # Analyzes stats and recommends indexes
├── workload_X_stats.json     # Auto-generated stats per workload
├── workload_X_alert.json     # Alerter decision for workload X
└── README.md

 Running the Workload Runner

This executes 4 phases of a workload and collects:
Baseline cost (no indexes)
Optimized cost (all hypothetical index combos)
Hot columns
Index usage
Index sizes

Command
python3 workload_runner.py <workload_id> <duration_per_phase_sec>

Example
python3 workload_runner.py 4 30

This runs Workload 4 for:
4 phases

30 seconds per phase
Total = 120 seconds
Ouput file generated:
workload_4_stats.json

 Running the Alerter

Once stats are collected, run:

python3 alerter.py <workload_id> <improvement_threshold> <space_budget_mb>

Example
python3 alerter.py 4 20 300


Where:

20 → minimum required improvement (%)
300 → max total index space allowed (MB)
Output file generated:
workload_4_alert.json
