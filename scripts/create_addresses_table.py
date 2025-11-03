import argparse
import sqlite3
import logging
from typing import Optional, List, Tuple, Dict
from dataclasses import dataclass
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressHierarchyBuilder:
    """
    Address hierarchy relationship builder - based on Prof. Michael Fuller's VB code logic
    Handles time segment splitting and multi-level belongs relationships
    Preserves gaps in data to tell the most continuous story possible
    """
    
    def __init__(self, db_path: str = "latest.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()
            
    def execute(self, sql: str, params: tuple = ()) -> int:
        """Execute SQL and return affected row count"""
        self.cursor.execute(sql, params)
        return self.cursor.rowcount
    
    def safe_min(self, *values):
        """Safe min function that ignores None values"""
        valid_values = [v for v in values if v is not None]
        return min(valid_values) if valid_values else None
    
    def safe_max(self, *values):
        """Safe max function that ignores None values"""
        valid_values = [v for v in values if v is not None]
        return max(valid_values) if valid_values else None
        
    def clean_belongs_data(self):
        """
        Clean invalid data in ADDR_BELONGS_DATA
        This is a key step in Michael's code
        """
        logger.info("Cleaning belongs data...")
        
        # Create temporary table to store cleaned data
        self.execute("DROP TABLE IF EXISTS CLEANED_BELONGS_DATA")
        self.execute("""
            CREATE TEMP TABLE CLEANED_BELONGS_DATA (
                c_addr_id INTEGER,
                c_belongs_to INTEGER,
                c_firstyear INTEGER,
                c_lastyear INTEGER
            )
        """)
        
        # Get all belongs relationships
        self.cursor.execute("""
            SELECT abd.*, 
                   ac1.c_firstyear as addr_first, 
                   ac1.c_lastyear as addr_last,
                   ac2.c_firstyear as belongs_first, 
                   ac2.c_lastyear as belongs_last
            FROM ADDR_BELONGS_DATA abd
            JOIN ADDR_CODES ac1 ON abd.c_addr_id = ac1.c_addr_id
            LEFT JOIN ADDR_CODES ac2 ON abd.c_belongs_to = ac2.c_addr_id
        """)
        
        rows = self.cursor.fetchall()
        valid_count = 0
        invalid_count = 0
        
        for row in rows:
            # Rule 1: Exclude Unknown (c_belongs_to = 0 or NULL)
            if not row['c_belongs_to'] or row['c_belongs_to'] == 0:
                invalid_count += 1
                continue
                
            # Rule 2: belongs_to unit must exist
            if row['belongs_first'] is None or row['belongs_last'] is None:
                logger.warning(f"Belongs_to unit {row['c_belongs_to']} does not exist")
                invalid_count += 1
                continue
            
            # Get time values, handle None cases
            abd_first = row['c_firstyear'] if row['c_firstyear'] is not None else row['addr_first']
            abd_last = row['c_lastyear'] if row['c_lastyear'] is not None else row['addr_last']
            
            # Calculate effective time range
            effective_first = self.safe_max(abd_first, row['addr_first'], row['belongs_first'])
            effective_last = self.safe_min(abd_last, row['addr_last'], row['belongs_last'])
            
            if effective_first is None or effective_last is None:
                logger.warning(f"Time range contains NULL values: {row['c_addr_id']} -> {row['c_belongs_to']}")
                invalid_count += 1
                continue
            
            if effective_first > effective_last:
                logger.warning(f"Invalid time range: {row['c_addr_id']} -> {row['c_belongs_to']} "
                             f"({effective_first} > {effective_last})")
                invalid_count += 1
                continue
                
            # Insert cleaned data
            self.execute("""
                INSERT INTO CLEANED_BELONGS_DATA 
                (c_addr_id, c_belongs_to, c_firstyear, c_lastyear)
                VALUES (?, ?, ?, ?)
            """, (row['c_addr_id'], row['c_belongs_to'], 
                  effective_first, effective_last))
            valid_count += 1
            
        logger.info(f"Data cleaning completed: {valid_count} valid, {invalid_count} invalid")
        
    def build_time_segments_with_gaps(self):
        """
        Build time segments including gaps
        This preserves the gaps in data and tells the most continuous story
        """
        logger.info("Building time segments with gap filling...")
        
        # Create result table
        self.execute("DROP TABLE IF EXISTS TIME_SEGMENTS")
        self.execute("""
            CREATE TEMP TABLE TIME_SEGMENTS (
                c_addr_id INTEGER,
                segment_start INTEGER,
                segment_end INTEGER,
                belongs_chain TEXT,
                level1_id INTEGER,
                level1_start INTEGER,
                level1_end INTEGER,
                level2_id INTEGER,
                level2_start INTEGER,
                level2_end INTEGER,
                level3_id INTEGER,
                level3_start INTEGER,
                level3_end INTEGER,
                level4_id INTEGER,
                level4_start INTEGER,
                level4_end INTEGER,
                level5_id INTEGER,
                level5_start INTEGER,
                level5_end INTEGER
            )
        """)
        
        # Get all addresses with valid year data
        self.cursor.execute("""
            SELECT c_addr_id, c_firstyear, c_lastyear 
            FROM ADDR_CODES 
            WHERE c_firstyear IS NOT NULL AND c_lastyear IS NOT NULL
        """)
        addresses = self.cursor.fetchall()
        
        logger.info(f"Processing {len(addresses)} addresses with valid year data...")
        
        for addr_row in addresses:
            addr_id = addr_row['c_addr_id']
            addr_first = addr_row['c_firstyear']
            addr_last = addr_row['c_lastyear']
            
            # Skip if years are invalid
            if addr_first is None or addr_last is None or addr_first > addr_last:
                logger.warning(f"Skipping address {addr_id} with invalid years: {addr_first}-{addr_last}")
                continue
            
            # Get all level 1 belongs relationships for this address
            self.cursor.execute("""
                SELECT DISTINCT c_belongs_to, c_firstyear, c_lastyear
                FROM CLEANED_BELONGS_DATA
                WHERE c_addr_id = ?
                ORDER BY c_firstyear
            """, (addr_id,))
            
            level1_belongs = self.cursor.fetchall()
            
            if not level1_belongs:
                # No belongs relationship for entire period
                self._insert_segment(addr_id, addr_first, addr_last, {})
            else:
                # Process each L1 relationship and fill gaps
                current_year = addr_first
                
                for l1 in level1_belongs:
                    l1_start = l1['c_firstyear']
                    l1_end = l1['c_lastyear']
                    l1_id = l1['c_belongs_to']
                    
                    # If there's a gap before this L1 relationship
                    if current_year < l1_start:
                        # Insert gap record with only L1 (no deeper levels)
                        gap_chain = {'level1': {
                            'id': l1_id,
                            'start': current_year,
                            'end': l1_start - 1
                        }}
                        self._insert_segment(addr_id, current_year, l1_start - 1, gap_chain)
                    
                    # Process the actual L1 period with its nested relationships
                    self._process_level1_with_gaps(addr_id, l1_id, l1_start, l1_end)
                    
                    current_year = l1_end + 1
                
                # Fill gap at the end if needed
                if addr_last is not None and current_year <= addr_last:
                    # Use the last L1 belongs for the gap
                    if level1_belongs:
                        last_l1 = level1_belongs[-1]
                        gap_chain = {'level1': {
                            'id': last_l1['c_belongs_to'],
                            'start': current_year,
                            'end': addr_last
                        }}
                        self._insert_segment(addr_id, current_year, addr_last, gap_chain)
    
    def _process_level1_with_gaps(self, addr_id: int, l1_id: int, l1_start: int, l1_end: int):
        """
        Process a Level 1 belongs period, filling gaps in Level 2+ relationships
        """
        if l1_start is None or l1_end is None:
            return
            
        # Get Level 2 relationships for this L1
        self.cursor.execute("""
            SELECT DISTINCT c_belongs_to, c_firstyear, c_lastyear
            FROM CLEANED_BELONGS_DATA
            WHERE c_addr_id = ? 
              AND c_firstyear <= ?
              AND c_lastyear >= ?
            ORDER BY c_firstyear
        """, (l1_id, l1_end, l1_start))
        
        level2_belongs = self.cursor.fetchall()
        
        if not level2_belongs:
            # No Level 2 for entire L1 period
            chain = {'level1': {'id': l1_id, 'start': l1_start, 'end': l1_end}}
            self._insert_segment(addr_id, l1_start, l1_end, chain)
        else:
            # Process L2 relationships and fill gaps
            current_year = l1_start
            
            for l2 in level2_belongs:
                # Calculate intersection with L1 period
                l2_effective_start = max(l2['c_firstyear'], l1_start)
                l2_effective_end = min(l2['c_lastyear'], l1_end)
                
                if l2_effective_start > l2_effective_end:
                    continue
                
                # Fill gap before this L2 if needed
                if current_year < l2_effective_start:
                    gap_chain = {
                        'level1': {'id': l1_id, 'start': current_year, 'end': l2_effective_start - 1}
                    }
                    self._insert_segment(addr_id, current_year, l2_effective_start - 1, gap_chain)
                
                # Process the actual L2 period with deeper levels
                self._process_level2_with_gaps(addr_id, l1_id, l1_start, l1_end,
                                              l2['c_belongs_to'], l2_effective_start, l2_effective_end)
                
                current_year = l2_effective_end + 1
            
            # Fill gap at the end of L1 period if needed
            if current_year <= l1_end:
                gap_chain = {
                    'level1': {'id': l1_id, 'start': current_year, 'end': l1_end}
                }
                self._insert_segment(addr_id, current_year, l1_end, gap_chain)
    
    def _process_level2_with_gaps(self, addr_id: int, l1_id: int, l1_start: int, l1_end: int,
                                 l2_id: int, l2_start: int, l2_end: int):
        """
        Process Level 2 and deeper, continuing to fill gaps
        """
        if l2_start is None or l2_end is None:
            return
            
        # Get Level 3 relationships
        self.cursor.execute("""
            SELECT DISTINCT c_belongs_to, c_firstyear, c_lastyear
            FROM CLEANED_BELONGS_DATA
            WHERE c_addr_id = ? 
              AND c_firstyear <= ?
              AND c_lastyear >= ?
            ORDER BY c_firstyear
        """, (l2_id, l2_end, l2_start))
        
        level3_belongs = self.cursor.fetchall()
        
        if not level3_belongs:
            # No Level 3 for entire L2 period
            chain = {
                'level1': {'id': l1_id, 'start': l1_start, 'end': l1_end},
                'level2': {'id': l2_id, 'start': l2_start, 'end': l2_end}
            }
            self._insert_segment(addr_id, l2_start, l2_end, chain)
        else:
            # Process L3 relationships and fill gaps
            current_year = l2_start
            
            for l3 in level3_belongs:
                # Calculate intersection
                l3_effective_start = max(l3['c_firstyear'], l2_start)
                l3_effective_end = min(l3['c_lastyear'], l2_end)
                
                if l3_effective_start > l3_effective_end:
                    continue
                
                # Fill gap before this L3
                if current_year < l3_effective_start:
                    gap_chain = {
                        'level1': {'id': l1_id, 'start': l1_start, 'end': l1_end},
                        'level2': {'id': l2_id, 'start': current_year, 'end': l3_effective_start - 1}
                    }
                    self._insert_segment(addr_id, current_year, l3_effective_start - 1, gap_chain)
                
                # Create segment with L3
                chain = {
                    'level1': {'id': l1_id, 'start': l1_start, 'end': l1_end},
                    'level2': {'id': l2_id, 'start': l2_start, 'end': l2_end},
                    'level3': {'id': l3['c_belongs_to'], 'start': l3_effective_start, 'end': l3_effective_end}
                }
                
                # Continue to L4 and L5 if needed
                self._process_deeper_levels(addr_id, chain, l3['c_belongs_to'], 
                                           l3_effective_start, l3_effective_end, 3)
                
                current_year = l3_effective_end + 1
            
            # Fill gap at end of L2 period
            if current_year <= l2_end:
                gap_chain = {
                    'level1': {'id': l1_id, 'start': l1_start, 'end': l1_end},
                    'level2': {'id': l2_id, 'start': current_year, 'end': l2_end}
                }
                self._insert_segment(addr_id, current_year, l2_end, gap_chain)
    
    def _process_deeper_levels(self, addr_id: int, chain: Dict, parent_id: int, 
                              start: int, end: int, current_level: int):
        """
        Generic processor for levels 4 and 5
        """
        if start is None or end is None:
            return
            
        if current_level >= 5:
            # Already at max depth, save the segment
            self._insert_segment(addr_id, start, end, chain)
            return
        
        next_level = current_level + 1
        
        # Get next level relationships
        self.cursor.execute("""
            SELECT DISTINCT c_belongs_to, c_firstyear, c_lastyear
            FROM CLEANED_BELONGS_DATA
            WHERE c_addr_id = ? 
              AND c_firstyear <= ?
              AND c_lastyear >= ?
            ORDER BY c_firstyear
        """, (parent_id, end, start))
        
        next_belongs = self.cursor.fetchall()
        
        if not next_belongs:
            # No deeper level, save current chain
            self._insert_segment(addr_id, start, end, chain)
        else:
            # Process with gaps
            current_year = start
            
            for nb in next_belongs:
                nb_start = max(nb['c_firstyear'], start)
                nb_end = min(nb['c_lastyear'], end)
                
                if nb_start > nb_end:
                    continue
                
                # Fill gap before
                if current_year < nb_start:
                    self._insert_segment(addr_id, current_year, nb_start - 1, chain)
                
                # Create new chain with next level
                new_chain = chain.copy()
                new_chain[f'level{next_level}'] = {
                    'id': nb['c_belongs_to'],
                    'start': nb_start,
                    'end': nb_end
                }
                
                # Continue deeper
                self._process_deeper_levels(addr_id, new_chain, nb['c_belongs_to'],
                                          nb_start, nb_end, next_level)
                
                current_year = nb_end + 1
            
            # Fill gap at end
            if current_year <= end:
                self._insert_segment(addr_id, current_year, end, chain)
                                               
    def _insert_segment(self, addr_id: int, start: int, end: int, chain: Dict):
        """Insert a time segment record"""
        if start is None or end is None:
            return
            
        values = [addr_id, start, end, str(chain)]
        
        # Add level information
        for i in range(1, 6):
            if f'level{i}' in chain:
                values.extend([
                    chain[f'level{i}']['id'],
                    chain[f'level{i}'].get('start', start),
                    chain[f'level{i}'].get('end', end)
                ])
            else:
                values.extend([None, None, None])
        
        placeholders = ','.join(['?' for _ in values])
        self.execute(f"""
            INSERT INTO TIME_SEGMENTS VALUES ({placeholders})
        """, tuple(values))
            
    def build_final_addresses_table(self):
        """Build final ADDRESSES table"""
        logger.info("Building final ADDRESSES table...")
        
        # Drop old table
        self.execute("DROP TABLE IF EXISTS ADDRESSES")
        
        # Create new table matching Michael's structure
        self.execute("""
            CREATE TABLE ADDRESSES (
                c_addr_id INTEGER,
                c_name TEXT,
                c_name_chn TEXT,
                c_admin_type TEXT,
                c_firstyear INTEGER,
                c_lastyear INTEGER,
                c_belongs_firstyear INTEGER,
                c_belongs_lastyear INTEGER,
                x_coord REAL,
                y_coord REAL,
                belongs1_ID INTEGER,
                belongs1_Name TEXT,
                belongs1_Name_chn TEXT,
                belongs2_ID INTEGER,
                belongs2_Name TEXT,
                belongs2_Name_chn TEXT,
                belongs3_ID INTEGER,
                belongs3_Name TEXT,
                belongs3_Name_chn TEXT,
                belongs4_ID INTEGER,
                belongs4_Name TEXT,
                belongs4_Name_chn TEXT,
                belongs5_ID INTEGER,
                belongs5_Name TEXT,
                belongs5_Name_chn TEXT
            )
        """)
        
        # Build final data from TIME_SEGMENTS
        self.execute("""
            INSERT INTO ADDRESSES
            SELECT 
                ts.c_addr_id,
                ac.c_name,
                ac.c_name_chn,
                ac.c_admin_type,
                ac.c_firstyear,
                ac.c_lastyear,
                ts.segment_start as c_belongs_firstyear,
                ts.segment_end as c_belongs_lastyear,
                ac.x_coord,
                ac.y_coord,
                ts.level1_id,
                a1.c_name,
                a1.c_name_chn,
                ts.level2_id,
                a2.c_name,
                a2.c_name_chn,
                ts.level3_id,
                a3.c_name,
                a3.c_name_chn,
                ts.level4_id,
                a4.c_name,
                a4.c_name_chn,
                ts.level5_id,
                a5.c_name,
                a5.c_name_chn
            FROM TIME_SEGMENTS ts
            JOIN ADDR_CODES ac ON ts.c_addr_id = ac.c_addr_id
            LEFT JOIN ADDR_CODES a1 ON ts.level1_id = a1.c_addr_id
            LEFT JOIN ADDR_CODES a2 ON ts.level2_id = a2.c_addr_id
            LEFT JOIN ADDR_CODES a3 ON ts.level3_id = a3.c_addr_id
            LEFT JOIN ADDR_CODES a4 ON ts.level4_id = a4.c_addr_id
            LEFT JOIN ADDR_CODES a5 ON ts.level5_id = a5.c_addr_id
            ORDER BY ts.c_addr_id, ts.segment_start
        """)
        
        count = self.cursor.rowcount
        logger.info(f"ADDRESSES table created with {count} records")
        
        # Verify example cases
        self._verify_example_cases()
        
    def _verify_example_cases(self):
        """Verify the specific cases mentioned in Michael's emails"""
        # Check Jiangle (100149)
        logger.info("Verifying Jiangle (100149)...")
        self.cursor.execute("""
            SELECT c_belongs_firstyear, c_belongs_lastyear, 
                   belongs1_Name_chn, belongs2_Name_chn, belongs3_Name_chn
            FROM ADDRESSES 
            WHERE c_addr_id = 100149 
            ORDER BY c_belongs_firstyear
        """)
        
        results = self.cursor.fetchall()
        if results:
            logger.info(f"Jiangle has {len(results)} records:")
            for row in results:
                logger.info(f"  {row['c_belongs_firstyear']}-{row['c_belongs_lastyear']}: "
                           f"{row['belongs1_Name_chn']} -> {row['belongs2_Name_chn'] or ''} -> "
                           f"{row['belongs3_Name_chn'] or ''}")
        
        # Check Jun county (4524) if it exists
        self.cursor.execute("""
            SELECT c_belongs_firstyear, c_belongs_lastyear,
                   belongs1_Name_chn, belongs2_Name_chn, belongs3_Name_chn, belongs4_Name_chn
            FROM ADDRESSES 
            WHERE c_addr_id = 4524 
            ORDER BY c_belongs_firstyear
            LIMIT 10
        """)
        
        results = self.cursor.fetchall()
        if results:
            logger.info(f"\nJun county (4524) has {len(results)} records (showing first 10):")
            for row in results:
                logger.info(f"  {row['c_belongs_firstyear']}-{row['c_belongs_lastyear']}: "
                           f"{row['belongs1_Name_chn']} -> {row['belongs2_Name_chn'] or ''} -> "
                           f"{row['belongs3_Name_chn'] or ''} -> {row['belongs4_Name_chn'] or ''}")
                       
    def run(self):
        """Execute complete build process"""
        try:
            logger.info("="*60)
            logger.info("Starting address hierarchy build with gap preservation...")
            logger.info("="*60)
            
            # 1. Clean data
            self.clean_belongs_data()
            
            # 2. Build time segments with gaps
            self.build_time_segments_with_gaps()
            
            # 3. Generate final table
            self.build_final_addresses_table()
            
            logger.info("="*60)
            logger.info("Build completed!")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Build process error: {e}")
            import traceback
            traceback.print_exc()
            raise

# Usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the ADDRESSES table from the CBDB SQLite database.")
    parser.add_argument("--db", default="latest.db", help="Path to the SQLite database file to process")
    args = parser.parse_args()

    with AddressHierarchyBuilder(args.db) as builder:
        builder.run()
