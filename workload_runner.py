"""
Workload Runner - Collects BASELINE and OPTIMIZED costs
BASELINE: Query cost without hypothetical indexes
OPTIMIZED: Query cost with ALL POSSIBLE hypothetical index combinations

Supports ANY database, ANY query (including JOINs)
Extracts tables and columns from query automatically
Creates all combinations and lets EXPLAIN pick the best
"""

import psycopg2
import json
import time
import itertools
import re
from datetime import datetime
import sys

class WorkloadRunner:
    def __init__(self, db_config, workload_id):
        self.conn = psycopg2.connect(**db_config)
        self.workload_id = workload_id
        self.stats = {
            'workload_id': workload_id,
            'run_time': datetime.now().isoformat(),
            'queries': {},  # Dict: query -> {count, baseline_cost, optimized_cost, improvement_pct, etc}
            'phases_completed': 0,
            'hot_columns': [],
            'last_save': datetime.now().isoformat()
        }
        self.query_hash_map = {}  # Map: query -> aggregated stats
        self.existing_indexes = self._get_existing_indexes()  # Cache existing indexes
    
    def _get_existing_indexes(self):
        """Get list of columns that already have indexes"""
        existing = set()
        try:
            with self.conn.cursor() as cur:
                # Get all indexed columns on the orders table
                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid
                        AND a.attnum = ANY(i.indkey)
                    JOIN pg_class t ON t.oid = i.indrelid
                    JOIN pg_class idx ON idx.oid = i.indexrelid
                    WHERE t.relname = 'orders'
                """)
                for row in cur.fetchall():
                    existing.add(row[0])
        except Exception as e:
            print(f"Warning: Could not get existing indexes: {e}")
        return existing
    
    def extract_tables_from_query(self, query):
        """Extract table names from query (FROM, JOIN clauses)"""
        tables = set()
        # Match FROM table_name
        from_pattern = r'FROM\s+(\w+)'
        tables.update(re.findall(from_pattern, query, re.IGNORECASE))
        
        # Match JOIN table_name
        join_pattern = r'JOIN\s+(\w+)'
        tables.update(re.findall(join_pattern, query, re.IGNORECASE))
        
        return list(tables)
    
    def extract_columns_from_query(self, query):
        """Extract column names from WHERE, JOIN ON, SELECT clauses"""
        columns = set()
        
        # Match table-qualified columns (e.g., o.o_orderkey, l.l_quantity)
        # Pattern: word.word (table.column)
        qualified_col_pattern = r'\b(\w+)\.(\w+)\b'
        matches = re.findall(qualified_col_pattern, query)
        for table_alias, col_name in matches:
            # Skip functions and keywords
            if col_name.lower() not in ['*', 'count', 'sum', 'max', 'min', 'avg', 'date', 'extract', 'cast']:
                columns.add(col_name.lower())
        
        return list(columns) if columns else []
    
    def get_baseline_cost(self, query):
        """Get BASELINE cost: query without new indexes"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
                result = cur.fetchone()
                if result and result[0]:
                    plan = result[0]
                    return float(plan[0]['Plan']['Total Cost'])
        except Exception as e:
            pass
        return None
    
    def get_optimized_cost(self, query, tables, columns):
        """Get OPTIMIZED cost with hypothetical indexes on NEW (non-indexed) columns only
        
        Only creates indexes on columns that DON'T already have real indexes.
        This shows the real improvement potential of new indexes.
        Also captures hypothetical index sizes using hypopg_relation_size().
        Tracks WHICH indexes were actually used by the planner in the EXPLAIN plan.
        Hypothetical indexes must be used in same session before drop.
        """
        try:
            with self.conn.cursor() as cur:
                # Load hypopg
                try:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS hypopg")
                    self.conn.commit()
                except:
                    pass
                
                if not tables or not columns:
                    return None
                
                # Filter: only get columns that DON'T have existing indexes
                new_columns = [col for col in columns if col not in self.existing_indexes]
                
                if not new_columns:
                    # No new columns to index, return baseline cost
                    return self.get_baseline_cost(query)
                
                # Get baseline cost first
                baseline_cost = self.get_baseline_cost(query)
                
                # Generate ALL combinations: single, pairs, triplets
                # This gives the planner more options to find the best plan
                all_combinations = []
                
                # Add all single columns
                for col in new_columns:
                    all_combinations.append([col])
                
                # Add all pairs of columns (2-column indexes)
                if len(new_columns) >= 2:
                    for combo in itertools.combinations(new_columns, 2):
                        all_combinations.append(list(combo))
                
                # Add all triplets of columns (3-column indexes)
                if len(new_columns) >= 3:
                    for combo in itertools.combinations(new_columns, 3):
                        all_combinations.append(list(combo))
                
                # Create hypothetical indexes on ALL tables x ALL combinations
                # IMPORTANT: Do NOT commit between creating indexes and using them!
                hyp_index_info = []  # List of (oid, index_name, size_bytes, columns)
                index_count = 0
                max_indexes = 50  # Limit to prevent overwhelming the planner
                
                for table in tables:
                    if index_count >= max_indexes:
                        break
                    for combo in all_combinations:
                        if index_count >= max_indexes:
                            break
                        try:
                            cols_str = ", ".join(combo)
                            create_stmt = f"CREATE INDEX ON {table} ({cols_str})"
                            cur.execute(f"SELECT * FROM hypopg_create_index('{create_stmt}')")
                            result = cur.fetchone()
                            if result:
                                oid = result[0]
                                index_name = result[1]
                                
                                # Get hypothetical index size using hypopg_relation_size()
                                try:
                                    cur.execute(f"SELECT hypopg_relation_size({oid})")
                                    size_result = cur.fetchone()
                                    index_size = size_result[0] if size_result else 0
                                except:
                                    index_size = 0
                                
                                hyp_index_info.append((oid, index_name, index_size, combo, table))
                                index_count += 1
                        except Exception as e:
                            pass
                
                # Get cost with ALL hypothetical indexes (in SAME session, before drop)
                cost = None
                used_indexes = []  # Will store which indexes were actually used
                if hyp_index_info:  # Only run EXPLAIN if we actually created indexes
                    try:
                        # CRITICAL: Query must run while hypothetical indexes are active
                        cur.execute(f"EXPLAIN (FORMAT JSON, VERBOSE) {query}")
                        result = cur.fetchone()
                        if result and result[0]:
                            plan = result[0]
                            cost = float(plan[0]['Plan']['Total Cost'])
                            
                            # Extract index names from the plan to see which were used
                            plan_str = json.dumps(plan)
                            for oid, index_name, index_size, cols, table in hyp_index_info:
                                # Check if this index name appears in the EXPLAIN plan
                                if index_name in plan_str:
                                    used_indexes.append({
                                        'index_name': index_name,
                                        'columns': cols,
                                        'table': table,
                                        'size_bytes': index_size
                                    })
                    except Exception as e:
                        pass
                
                # Store index info in stats for analysis
                if not hasattr(self, 'index_info'):
                    self.index_info = {
                        'all_created': [],    # All hypothetical indexes we created
                        'used_by_query': []   # Which ones were actually used
                    }
                
                self.index_info['all_created'] = [
                    {
                        'index_name': idx_name,
                        'columns': cols,
                        'table': tbl,
                        'size_bytes': size
                    }
                    for oid, idx_name, size, cols, tbl in hyp_index_info
                ]
                self.index_info['used_by_query'] = used_indexes
                
                # Clean up ALL hypothetical indexes immediately after
                for oid, _, _, _, _ in hyp_index_info:
                    try:
                        cur.execute(f"SELECT * FROM hypopg_drop_index({oid})")
                    except:
                        pass
                
                # Commit after cleanup
                self.conn.commit()
                
                # If we got a cost, return it. Otherwise fall back to baseline
                if cost is not None:
                    return cost
                else:
                    return baseline_cost
        except Exception as e:
            pass
        return None
    
    def get_phase_queries(self, phase_num):
        """Get queries for phase (different patterns for different phases) """
        if self.workload_id == 1:
            if phase_num == 0:  # PHASE 0: Seq Scan queries - no indexes on these columns yet
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_totalprice > 150000 AND o.o_shippriority = 1",
                    "SELECT o.o_orderkey, o.o_totalprice FROM orders o WHERE o.o_totalprice BETWEEN 100000 AND 250000 AND o.o_shippriority IN (0, 1) AND o.o_shipmode = 'RAIL'",
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shippriority = 2 AND o.o_shipmode = 'AIR' AND o.o_totalprice > 80000",
                ]
            elif phase_num == 1:
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_totalprice > 200000 AND o.o_shippriority > 0 AND o.o_shipmode = 'SHIP'",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_shippriority = 1 AND o.o_shipmode IN ('TRUCK', 'RAIL') AND o.o_totalprice > 120000",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_shippriority IN (0, 1, 2) AND o.o_shipmode = 'FOB'",
                ]
            elif phase_num == 2:  # PHASE 2: Multi-column indexable queries
                return [
                    "SELECT SUM(o.o_totalprice) FROM orders o WHERE o.o_shippriority > 1 AND o.o_shipmode = 'HAND' AND o.o_totalprice > 100000",
                    "SELECT o.o_orderkey, COUNT(*) FROM orders o WHERE o.o_totalprice BETWEEN 50000 AND 150000 AND o.o_shippriority = 1 GROUP BY o.o_orderkey",
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shipmode IN ('MAIL', 'TRUCK') AND o.o_totalprice > 180000",
                ]
            else:  # PHASE 3: More complex multi-column queries
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shippriority IN (1, 2) AND o.o_shipmode = 'RAIL' AND o.o_totalprice > 200000",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_totalprice BETWEEN 75000 AND 225000 AND o.o_shippriority > 0",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_shipmode = 'AIR' AND o.o_totalprice > 160000 AND o.o_shippriority = 1",
                ]
        
        elif self.workload_id == 2:
            if phase_num == 0:  # PHASE 0: o_totalprice + o_custkey (Seq Scan queries)
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_totalprice > 150000 AND o.o_custkey > 1000",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_totalprice BETWEEN 100000 AND 200000 AND o.o_shippriority > 0",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_custkey < 500 AND o.o_shipmode = 'SHIP'",
                ]
            elif phase_num == 1:
                return [
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_totalprice > 200000 AND o.o_shippriority IN (0, 1, 2)",
                    "SELECT COUNT(*) FROM orders o WHERE o.o_custkey IN (100, 200, 300, 400, 500) AND o.o_totalprice > 50000",
                    "SELECT SUM(o.o_totalprice) FROM orders o WHERE o.o_totalprice BETWEEN 50000 AND 100000 AND o.o_custkey > 1000",
                ]
            elif phase_num == 2:  # PHASE 2: SHIFT to o_shipmode + o_shippriority
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shipmode = 'RAIL' AND o.o_totalprice > 100000",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_shipmode IN ('AIR', 'SHIP') AND o.o_shippriority > 0",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_shipmode = 'TRUCK' AND o.o_custkey > 5000",
                ]
            else:  # PHASE 3: Complex multi-column queries
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shipmode = 'FOB' AND o.o_shippriority = 1 AND o.o_totalprice > 120000",
                    "SELECT o.o_orderkey, o.o_totalprice FROM orders o WHERE o.o_shipmode IN ('MAIL', 'HAND') AND o.o_custkey < 3000",
                    "SELECT SUM(o.o_totalprice) FROM orders o WHERE o.o_shippriority > 0 AND o.o_shipmode = 'RAIL'",
                ]
        
        elif self.workload_id == 3:
            if phase_num == 0:  # PHASE 0: o_custkey  (Seq Scan queries)
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_custkey = 100 AND o.o_totalprice > 50000",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_custkey > 1000 AND o.o_custkey < 2000 AND o.o_shippriority > 0",
                    "SELECT o.o_totalprice FROM orders o WHERE o.o_custkey IN (50, 100, 150, 200, 250) AND o.o_shipmode = 'AIR'",
                ]
            elif phase_num == 1:
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_custkey = 500 AND o.o_shipmode IN ('SHIP', 'TRUCK')",
                    "SELECT o.o_totalprice FROM orders o WHERE o.o_custkey > 5000 AND o.o_totalprice > 100000",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_custkey BETWEEN 1000 AND 3000 AND o.o_shippriority > 0",
                ]
            elif phase_num == 2:  # PHASE 2: SHIFT to o_shippriority + o_shipmode
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shippriority > 0 AND o.o_shipmode = 'AIR' AND o.o_totalprice > 80000",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_shippriority IN (1, 2) AND o.o_shipmode = 'SHIP' AND o.o_custkey > 2000",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_shippriority = 1 AND o.o_shipmode = 'TRUCK'",
                ]
            else:  # PHASE 3: Continue
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shippriority = 2 AND o.o_shipmode IN ('RAIL', 'HAND') AND o.o_totalprice > 100000",
                    "SELECT o.o_orderkey, o.o_totalprice FROM orders o WHERE o.o_shippriority > 0 AND o.o_custkey < 8000",
                    "SELECT SUM(o.o_totalprice) FROM orders o WHERE o.o_shipmode = 'FOB' AND o.o_shippriority > 1",
                ]
        
        elif self.workload_id == 4:
            if phase_num == 0:  # PHASE 0: o_totalprice focus (Seq Scan queries)
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_totalprice > 250000 AND o.o_shippriority > 0",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_totalprice BETWEEN 150000 AND 300000 AND o.o_custkey > 1000",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_totalprice < 50000 AND o.o_shipmode = 'RAIL'",
                ]
            elif phase_num == 1:
                return [
                    "SELECT o.o_orderkey, o.o_totalprice FROM orders o WHERE o.o_totalprice > 100000 AND o.o_custkey < 5000",
                    "SELECT COUNT(*) FROM orders o WHERE o.o_totalprice BETWEEN 200000 AND 400000 AND o.o_shippriority IN (0, 1, 2)",
                    "SELECT SUM(o.o_totalprice) FROM orders o WHERE o.o_totalprice = 150000 AND o.o_shipmode = 'SHIP'",
                ]
            elif phase_num == 2:  # PHASE 2: SHIFT to o_shippriority + o_shipmode focus
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shippriority > 0 AND o.o_totalprice > 100000 AND o.o_shipmode IN ('RAIL', 'AIR')",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_shippriority IN (1, 2) AND o.o_totalprice < 200000",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_shippriority = 1 AND o.o_custkey > 2000",
                ]
            else:  # PHASE 3: Continue
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shippriority > 0 AND o.o_shipmode = 'RAIL' AND o.o_totalprice > 80000",
                    "SELECT o.o_totalprice FROM orders o WHERE o.o_shippriority = 0 AND o.o_custkey BETWEEN 1000 AND 5000",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_shipmode IN ('TRUCK', 'SHIP', 'AIR') AND o.o_totalprice > 100000",
                ]
        
        elif self.workload_id == 5:
            if phase_num == 0:  # PHASE 0: o_custkey + o_totalprice (Seq Scan queries)
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_custkey > 1000 AND o.o_totalprice > 100000 AND o.o_shippriority > 0",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_custkey = 500 AND o.o_totalprice < 150000",
                    "SELECT SUM(o.o_totalprice) FROM orders o WHERE o.o_custkey BETWEEN 1000 AND 5000 AND o.o_shipmode = 'TRUCK'",
                ]
            elif phase_num == 1:
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_custkey < 2000 AND o.o_totalprice > 50000",
                    "SELECT o.o_totalprice FROM orders o WHERE o.o_custkey > 5000 AND o.o_shippriority IN (1, 2)",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_custkey IN (100, 200, 300, 400) AND o.o_totalprice BETWEEN 80000 AND 200000",
                ]
            elif phase_num == 2:  # PHASE 2: SHIFT to o_shipmode + o_totalprice
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shipmode = 'SHIP' AND o.o_totalprice > 100000 AND o.o_custkey > 2000",
                    "SELECT o.o_orderkey FROM orders o WHERE o.o_shipmode IN ('AIR', 'TRUCK') AND o.o_totalprice < 200000",
                    "SELECT AVG(o.o_totalprice) FROM orders o WHERE o.o_shipmode = 'MAIL' AND o.o_shippriority > 0",
                ]
            else:  # PHASE 3: Continue
                return [
                    "SELECT COUNT(*) FROM orders o WHERE o.o_shipmode = 'RAIL' OR o.o_shipmode = 'FOB' AND o.o_totalprice > 120000",
                    "SELECT o.o_orderkey, o.o_totalprice FROM orders o WHERE o.o_shipmode = 'HAND' AND o.o_totalprice > 150000",
                    "SELECT SUM(o.o_totalprice) FROM orders o WHERE o.o_shipmode IN ('SHIP', 'AIR', 'TRUCK', 'RAIL') AND o.o_custkey < 8000",
                ]
        
        else:
            # Default fallback queries
            return [
                "SELECT COUNT(*) FROM orders WHERE o_shippriority > 0",
                "SELECT COUNT(*) FROM orders WHERE o_totalprice > 100000",
                "SELECT * FROM orders WHERE o_shipmode = 'SHIP' LIMIT 100",
            ]
    
    def run_phase(self, phase_num, duration_sec):
        """Run queries for one phase"""
        phase_name = ['Phase 0', 'Phase 1', 'Phase 2', 'Phase 3'][phase_num]
        print(f"\n{'='*80}")
        print(f"Workload {self.workload_id} - {phase_name}")
        print(f"Duration: {duration_sec}s | Collecting stats...")
        print(f"{'='*80}")
        
        queries = self.get_phase_queries(phase_num)
        start_time = time.time()
        query_count = 0
        all_hot_cols = set()
        last_save_time = start_time
        save_interval = 10  # Save every 10 seconds
        
        while time.time() - start_time < duration_sec:
            for query in queries:
                # Extract tables and columns
                tables = self.extract_tables_from_query(query)
                columns = self.extract_columns_from_query(query)
                
                for col in columns:
                    all_hot_cols.add(col)
                
                if not tables or not columns:
                    continue
                
                # Get BASELINE cost
                baseline_cost = self.get_baseline_cost(query)
                if baseline_cost is None:
                    continue
                
                # Get OPTIMIZED cost (with all hypothetical index combinations)
                # This also populates self.index_info with used_by_query
                optimized_cost = self.get_optimized_cost(query, tables, columns)
                if optimized_cost is None:
                    continue
                
                # Calculate improvement based ONLY on indexes actually used
                if baseline_cost > 0:
                    improvement_pct = ((baseline_cost - optimized_cost) / baseline_cost) * 100
                else:
                    improvement_pct = 0
                
                # Get the indexes that were actually used
                used_indexes = self.index_info['used_by_query'] if hasattr(self, 'index_info') else []
                
                # Aggregate: use query (trimmed) as key
                query_key = query[:100]
                
                if query_key not in self.stats['queries']:
                    self.stats['queries'][query_key] = {
                        'full_query': query,
                        'tables': tables,
                        'hot_columns': columns,
                        'executions': 0,
                        'total_baseline_cost': 0.0,
                        'total_optimized_cost': 0.0,
                        'avg_improvement_pct': 0.0,
                        'min_improvement_pct': 100.0,
                        'max_improvement_pct': -100.0,
                        'total_index_size_bytes': 0,
                        'used_indexes': [],  # Track which indexes were actually used
                    }
                
                # Update aggregated stats
                entry = self.stats['queries'][query_key]
                entry['executions'] += 1
                entry['total_baseline_cost'] += baseline_cost
                entry['total_optimized_cost'] += optimized_cost
                entry['min_improvement_pct'] = min(entry['min_improvement_pct'], improvement_pct)
                entry['max_improvement_pct'] = max(entry['max_improvement_pct'], improvement_pct)
                entry['avg_improvement_pct'] = ((entry['total_baseline_cost'] - entry['total_optimized_cost']) / 
                                                 entry['total_baseline_cost'] * 100) if entry['total_baseline_cost'] > 0 else 0
                
                # Add index sizes and used indexes from this execution
                if hasattr(self, 'index_info'):
                    entry['used_indexes'] = self.index_info['used_by_query']
                    # Calculate total size only for indexes actually used
                    for idx in self.index_info['used_by_query']:
                        entry['total_index_size_bytes'] += idx['size_bytes']
                
                query_count += 1
                
                # Periodic save (every 10 seconds)
                current_time = time.time()
                if current_time - last_save_time >= save_interval:
                    self.stats['hot_columns'] = sorted(list(all_hot_cols))
                    self.save_stats()
                    last_save_time = current_time
                
                if query_count % 3 == 0:
                    elapsed = int(current_time - start_time)
                    hot_cols_str = ', '.join(sorted(list(all_hot_cols))[:3])
                    unique_queries = len(self.stats['queries'])
                    print(f"  Time: {elapsed}s | Executions: {query_count} | Unique Queries: {unique_queries} | Hot cols: {hot_cols_str}...", end='\r')
        
        # Update stats
        self.stats['phases_completed'] += 1
        self.stats['hot_columns'] = sorted(list(all_hot_cols))
        
        print(f"\n     Phase {phase_num} complete")
        print(f"    Total executions: {query_count}")
        print(f"    Unique queries: {len(self.stats['queries'])}")
        print(f"    Hot columns: {', '.join(sorted(all_hot_cols))}")
    
    def run_workload(self, duration_per_phase_sec):
        """Run all 4 phases"""
        print(f"\n{'#'*80}")
        print(f"# WORKLOAD {self.workload_id}")
        print(f"# 4 phases × {duration_per_phase_sec}s = {4 * duration_per_phase_sec}s total")
        print(f"{'#'*80}\n")
        
        try:
            for phase in range(4):
                self.run_phase(phase, duration_per_phase_sec)
                if phase < 3:
                    print(f"  Waiting 2s...")
                    time.sleep(2)
        finally:
            # ALWAYS save stats, even if interrupted
            self.save_stats()
            
            # Print final summary
            import os
            filename = f"workload_{self.workload_id}_stats.json"
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
                print(f"\n{'='*80}")
                print(f"   FINAL STATS: {filename}")
                print(f"  File size: {file_size / 1_000_000:.2f} MB")
                print(f"  Unique queries: {len(self.stats['queries'])}")
                print(f"  Total executions: {sum(q['executions'] for q in self.stats['queries'].values())}")
                print(f"  Phases: {self.stats['phases_completed']}")
                print(f"  Hot columns: {', '.join(self.stats['hot_columns'][:5])}...")
                print(f"{'='*80}\n")
    
    def save_stats(self):
        """Save stats to JSON file (incremental, readable format)"""
        filename = f"workload_{self.workload_id}_stats.json"
        try:
            with open(filename, 'w') as f:
                json.dump(self.stats, f, indent=2)  # Readable format with indentation
            
            self.stats['last_save'] = datetime.now().isoformat()
            return True
        except Exception as e:
            print(f"\n      ERROR saving stats: {e}")
            return False
    
    def close(self):
        self.conn.close()


if __name__ == '__main__':
    db_config = {
        'host': 'localhost',
        'port': 5433,
        'database': 'alerter_db',
        'user': 'amoljadhav',
        'password': 'postgres'
    }
    
    workload_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    duration_sec = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    
    print(f"\n{'*'*80}")
    print(f"*** WORKLOAD {workload_id} ***")
    print(f"Duration: {4 * duration_sec}s (4 phases × {duration_sec}s each)")
    print(f"{'*'*80}\n")
    
    runner = WorkloadRunner(db_config, workload_id)
    try:
        runner.run_workload(duration_sec)
    except Exception as e:
        print(f"\n    Error: {e}\n")
    finally:
        runner.close()