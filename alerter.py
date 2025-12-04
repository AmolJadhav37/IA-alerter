"""
Alerter - DUMB analyzer that reads pre-collected stats
Workload collects: baseline_cost (no indexes) + optimized_cost (with all hypothetical indexes)

Alerter just reads and decides:
  - Total improvement value = sum(improvement_pct% * baseline_cost for all queries)
  - Re-tuning cost = CREATE + DROP cost for hot columns
  - Decision: IF total_improvement > retuning_cost AND improvement% >= threshold THEN CREATE_INDEXES
"""

import json
import sys
import psycopg2
import time
from datetime import datetime

class Alerter:
    """Dumb alerter - just reads stats and decides YES/NO"""
    
    def __init__(self, db_config, improvement_threshold=20, max_space_budget_mb=500):
        self.conn = psycopg2.connect(**db_config)
        self.improvement_threshold = improvement_threshold  # percent (configurable)
        self.max_space_budget = max_space_budget_mb * 1_000_000  # Convert MB to bytes
    
    def get_existing_indexes(self):
        """Get list of existing indexes (do NOT recommend creating them again)"""
        existing = set()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT schemaname, tablename, indexname 
                FROM pg_indexes 
                WHERE tablename = 'orders' AND schemaname = 'public'
            """)
            for schema, table, idx in cur.fetchall():
                # Extract column from index name (e.g., idx_o_custkey -> o_custkey)
                if 'idx_' in idx:
                    col = idx.replace('idx_', '').replace('idx_opt_', '')
                    existing.add(col)
        return existing
    
    def get_real_index_size(self, column):
        """Calculate real index size by creating temp index and checking its size
        
        Creates a temporary index, measures its actual disk size using pg_relation_size(),
        then immediately drops it. No permanent changes to the database.
        """
        try:
            with self.conn.cursor() as cur:
                # Create a temporary index
                temp_idx_name = f"temp_idx_size_{column}_{int(time.time() * 1000000)}"
                try:
                    cur.execute(f"CREATE INDEX {temp_idx_name} ON orders ({column})")
                    self.conn.commit()
                    
                    # Get size in bytes
                    cur.execute(f"""
                        SELECT pg_relation_size('{temp_idx_name}'::regclass)
                    """)
                    result = cur.fetchone()
                    size_bytes = result[0] if result else 5_000_000
                    
                    # Drop temp index immediately
                    cur.execute(f"DROP INDEX {temp_idx_name}")
                    self.conn.commit()
                    
                    return size_bytes
                except Exception as e:
                    print(f"    Warning: Could not measure size for {column}: {e}")
                    # Fallback to estimate
                    return 5_000_000
        except Exception as e:
            print(f"    Warning: Could not create temp index for {column}: {e}")
            return 5_000_000
    
    def estimate_create_index_cost(self):
        """Estimate cost to CREATE index"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("EXPLAIN (FORMAT JSON) SELECT COUNT(*) FROM orders")
                result = cur.fetchone()
                if result:
                    plan = result[0]
                    return float(plan[0]['Plan']['Total Cost'])
        except:
            pass
        return 1000  # Fallback
    
    def estimate_drop_index_cost(self):
        """Estimate cost to DROP index"""
        return self.estimate_create_index_cost() * 0.2
    
    def load_workload_stats(self, workload_id):
        """Load stats file for workload"""
        filename = f"workload_{workload_id}_stats.json"
        try:
            with open(filename, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return None
    
    def extract_hot_columns_from_measurements(self, measurements):
        """Extract hot columns from measurements (already collected by workload)"""
        column_counts = {}
        for m in measurements:
            hot_cols = m.get('hot_columns', [])
            for col in hot_cols:
                column_counts[col] = column_counts.get(col, 0) + 1
        
        # Sort by frequency
        return sorted(column_counts.items(), key=lambda x: x[1], reverse=True)
    
    def analyze_workload(self, workload_id):
        """Analyze stats and make decision"""
        print(f"\n{'='*80}")
        print(f"ALERTER ANALYSIS - Workload {workload_id}")
        print(f"Time: {datetime.now().isoformat()}")
        print(f"{'='*80}\n")
        
        stats = self.load_workload_stats(workload_id)
        if not stats:
            print(f"   No stats file found for workload {workload_id}")
            return None
        
        # Support both old measurements format and new queries dict format
        queries_dict = stats.get('queries', {})
        measurements = stats.get('measurements', [])
        
        if not queries_dict and not measurements:
            print(f"   No measurements in stats file")
            return None
        
        print(f"Collected Data:")
        if queries_dict:
            unique_queries = len(queries_dict)
            total_executions = sum(q.get('executions', 0) for q in queries_dict.values())
            print(f"  - Unique queries: {unique_queries}")
            print(f"  - Total executions: {total_executions}")
        else:
            print(f"  - Measurements: {len(measurements)}")
        print(f"  - Phases completed: {stats.get('phases_completed', 0)}")
        print(f"  - Last update: {stats.get('last_update', 'unknown')}")
        
        # Calculate stats from queries dict (new format)
        total_baseline_cost = 0
        total_improvement_cost = 0  # How much we save with indexes
        all_improvement_pcts = []
        hot_columns_all_dict = {}
        
        if queries_dict:
            for query_key, query_data in queries_dict.items():
                baseline = query_data.get('total_baseline_cost', 0)
                optimized = query_data.get('total_optimized_cost', 0)
                improvement_pct = query_data.get('avg_improvement_pct', 0)
                hot_cols = query_data.get('hot_columns', [])
                
                total_baseline_cost += baseline
                total_improvement_cost += (baseline - optimized)
                all_improvement_pcts.append(improvement_pct)
                
                # Count hot columns
                for col in hot_cols:
                    hot_columns_all_dict[col] = hot_columns_all_dict.get(col, 0) + 1
        else:
            # Fallback to old measurements format
            for m in measurements:
                baseline = m.get('baseline_cost', 0)
                optimized = m.get('optimized_cost', 0)
                improvement_pct = m.get('improvement_pct', 0)
                
                total_baseline_cost += baseline
                total_improvement_cost += (baseline - optimized)
                all_improvement_pcts.append(improvement_pct)
        
        # Calculate average improvement
        avg_improvement_pct = sum(all_improvement_pcts) / len(all_improvement_pcts) if all_improvement_pcts else 0
        
        print(f"\nMeasured Statistics:")
        print(f"  - Total baseline cost (all queries, no indexes): {total_baseline_cost:.2f}")
        print(f"  - Total cost with hypothetical indexes: {total_baseline_cost - total_improvement_cost:.2f}")
        print(f"  - Total improvement value: {total_improvement_cost:.2f}")
        print(f"  - Average improvement per query: {avg_improvement_pct:.2f}%")
        
        # Get existing indexes
        existing = self.get_existing_indexes()
        print(f"\nExisting indexes (skip these):")
        if existing:
            print(f"  - {', '.join(sorted(existing))}")
        else:
            print(f"  - None")
        
        # Extract hot columns (from dict if available)
        if hot_columns_all_dict:
            hot_columns_all = sorted(hot_columns_all_dict.items(), key=lambda x: x[1], reverse=True)
        else:
            hot_columns_all = self.extract_hot_columns_from_measurements(measurements)
        
        candidates = [col for col, count in hot_columns_all if col not in existing][:3]  # Top 3 new indexes
        
        print(f"\nHot columns (frequency in workload):")
        for i, (col, count) in enumerate(hot_columns_all[:5], 1):
            marker = " ← TO INDEX" if col in candidates else ""
            print(f"  {i}. {col}: {count} queries{marker}")
        
        if not candidates:
            print(f"\n   All hot columns already indexed")
            recommendation = {
                'timestamp': datetime.now().isoformat(),
                'workload_id': workload_id,
                'decision': 'NO_ACTION',
                'reason': 'All hot columns already indexed',
                'improvement_pct': round(avg_improvement_pct, 2),
                'improvement_value': round(total_improvement_cost, 2),
                'retuning_cost': 0,
                'net_benefit': 0,
                'recommended_indexes': []
            }
            return recommendation
        
        # Calculate retuning cost (CREATE + DROP for each new index)
        total_retuning_cost = 0
        index_sizes = {}
        for col in candidates:
            create_cost = self.estimate_create_index_cost()
            drop_cost = self.estimate_drop_index_cost()
            size = self.get_real_index_size(col)  # Get REAL index size
            index_sizes[col] = size
            total_retuning_cost += create_cost + drop_cost
        
        # Net benefit
        net_benefit = total_improvement_cost - total_retuning_cost
        
        print(f"\nCost Analysis:")
        print(f"  - Improvement value (cost saved): {total_improvement_cost:.2f}")
        print(f"  - Re-tuning cost (create + drop): {total_retuning_cost:.2f}")
        print(f"  - Net benefit: {net_benefit:.2f}")
        
        # Space check
        total_new_space = sum(index_sizes.values())
        print(f"\nSpace Analysis:")
        print(f"  - Max budget: {self.max_space_budget / 1_000_000:.0f} MB")
        print(f"  - Estimated new indexes: {total_new_space / 1_000_000:.2f} MB")
        print(f"  - Fits budget: {'YES  ' if total_new_space <= self.max_space_budget else 'NO   '}")
        
        # Decision logic
        decision = 'NO_ACTION'
        reason = ''
        
        if avg_improvement_pct < self.improvement_threshold:
            decision = 'NO_ACTION'
            reason = f"Average improvement {avg_improvement_pct:.2f}% < threshold {self.improvement_threshold}%"
        elif net_benefit <= 0:
            decision = 'NO_ACTION'
            reason = f"Net benefit {net_benefit:.2f} <= 0 (retuning cost exceeds savings)"
        elif total_new_space > self.max_space_budget:
            decision = 'NO_ACTION'
            reason = f"Indexes {total_new_space/1_000_000:.0f}MB exceed budget {self.max_space_budget/1_000_000:.0f}MB"
        else:
            decision = 'CREATE_INDEXES'
            reason = f"Improvement {avg_improvement_pct:.2f}% >= {self.improvement_threshold}% AND net benefit {net_benefit:.2f} > 0"
        
        # Build recommendation
        recommended_indexes = []
        if decision == 'CREATE_INDEXES':
            for col in candidates:
                recommended_indexes.append({
                    'name': f"idx_opt_{col}",
                    'column': col,
                    'estimated_size_mb': index_sizes[col] / 1_000_000
                })
        
        recommendation = {
            'timestamp': datetime.now().isoformat(),
            'workload_id': workload_id,
            'decision': decision,
            'reason': reason,
            'improvement_pct': round(avg_improvement_pct, 2),
            'improvement_value': round(total_improvement_cost, 2),
            'retuning_cost': round(total_retuning_cost, 2),
            'net_benefit': round(net_benefit, 2),
            'recommended_indexes': recommended_indexes
        }
        
        # Display decision
        print(f"\n{'='*80}")
        print(f"DECISION: {decision}")
        print(f"Reason: {reason}")
        print(f"{'='*80}\n")
        
        if recommended_indexes:
            print(f"RECOMMENDED INDEXES:")
            for idx in recommended_indexes:
                print(f"  CREATE INDEX {idx['name']} ON orders ({idx['column']});")
            print()
        
        return recommendation
    
    def save_recommendation(self, workload_id, recommendation):
        """Save recommendation to file"""
        filename = f"workload_{workload_id}_alert.json"
        with open(filename, 'w') as f:
            json.dump(recommendation, f, indent=2, default=str)
        print(f"  Recommendation saved: {filename}\n")
    
    def close(self):
        self.conn.close()


if __name__ == '__main__':
    db_config = {
        'host': 'localhost',
        'port': 5433,
        'database': 'tpch',
        'user': 'amoljadhav',
        'password': 'postgres'
    }
    
    workload_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    improvement_threshold = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    space_budget_mb = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    
    print(f"\n{'='*80}")
    print(f"Alerter Configuration:")
    print(f"  - Workload ID: {workload_id}")
    print(f"  - Improvement threshold: {improvement_threshold}%")
    print(f"  - Space budget: {space_budget_mb} MB")
    print(f"{'='*80}\n")
    
    alerter = Alerter(db_config, improvement_threshold=improvement_threshold, max_space_budget_mb=space_budget_mb)
    try:
        recommendation = alerter.analyze_workload(workload_id)
        if recommendation:
            alerter.save_recommendation(workload_id, recommendation)
    finally:
        alerter.close()