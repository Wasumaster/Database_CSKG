import psycopg2
from psycopg2.extras import execute_values
import argparse
import time
import sys
from tqdm import tqdm  # Added progress bar

def execute_batches(cur, nodes_batch, edges_batch):
    """Common method for data batches with improved error handling"""
    try:
        if nodes_batch:
            execute_values(
                cur,
                """INSERT INTO nodes (node_id, node_label) 
                   VALUES %s 
                   ON CONFLICT (node_id) DO UPDATE 
                   SET node_label = EXCLUDED.node_label 
                   WHERE LENGTH(EXCLUDED.node_label) < LENGTH(nodes.node_label)""",
                nodes_batch,
                page_size=1000
            )
        
        if edges_batch:
            execute_values(
                cur,
                """INSERT INTO edges (edge_id, node1_id, node2_id, relation, relation_label)
                   VALUES %s
                   ON CONFLICT (edge_id) DO NOTHING""",  # Added conflict handling
                edges_batch,
                page_size=1000
            )
    except Exception as e:
        print(f"Error during batch execution: {str(e)}")
        raise

def count_lines(file_path):
    """Function to count lines in file for progress bar"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

def import_data(tsv_path, batch_size=50000, clean=False):
    """Improved data import function"""
    # Statistics
    total_nodes = 0
    total_edges = 0
    skipped_lines = 0
    
    try:
        # Database connection with timeout
        conn = psycopg2.connect(
            dbname="Projects2025",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432",
            connect_timeout=10
        )
        conn.autocommit = False
        cur = conn.cursor()

        # Database preparation
        if clean:
            print("Clearing existing data...")
            cur.execute("TRUNCATE TABLE edges, nodes RESTART IDENTITY CASCADE")
            conn.commit()

        # Temporary removal of indexes and constraints for performance
        print("Optimizing table structure...")
        cur.execute("""
            ALTER TABLE edges DROP CONSTRAINT IF EXISTS edges_node1_id_fkey;
            ALTER TABLE edges DROP CONSTRAINT IF EXISTS edges_node2_id_fkey;
            DROP INDEX IF EXISTS node1_idx, node2_idx, node_idx;
        """)
        conn.commit()

        # Data import
        print(f"Starting import from file: {tsv_path}")
        total_lines = count_lines(tsv_path) - 1  # Skip header
        edge_id = 0
        nodes_cache = set()
        nodes_batch = []
        edges_batch = []

        with open(tsv_path, 'r', encoding='utf-8') as f, \
             tqdm(total=total_lines, desc="Importing data") as pbar:

            header = f.readline()  # Skip header

            for line in f:
                try:
                    fields = line.strip().split('\t')
                    if len(fields) < 7:  # Minimum required number of fields
                        skipped_lines += 1
                        continue

                    node1_id, node1_label = fields[1], fields[4]
                    node2_id, node2_label = fields[3], fields[5]
                    relation, relation_label = fields[2], fields[6]

                    # Adding nodes
                    if node1_id not in nodes_cache:
                        nodes_batch.append((node1_id, node1_label))
                        nodes_cache.add(node1_id)
                        total_nodes += 1
                        
                    if node2_id not in nodes_cache:
                        nodes_batch.append((node2_id, node2_label))
                        nodes_cache.add(node2_id)
                        total_nodes += 1

                    # Adding edges
                    edges_batch.append((edge_id, node1_id, node2_id, relation, relation_label))
                    edge_id += 1
                    total_edges += 1

                    # Execute batch
                    if len(edges_batch) >= batch_size:
                        execute_batches(cur, nodes_batch, edges_batch)
                        conn.commit()
                        nodes_batch = []
                        edges_batch = []
                        # Don't clear node cache to avoid duplicates

                    pbar.update(1)

                except Exception as e:
                    print(f"Error in line: {line[:100]}... | {str(e)}")
                    skipped_lines += 1

            # Final batch
            if edges_batch:
                execute_batches(cur, nodes_batch, edges_batch)
                conn.commit()

    except Exception as e:
        print(f"Critical import error: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        # Restore database structure
        if 'cur' in locals() and 'conn' in locals():
            print("Restoring indexes and constraints...")
            try:
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS node1_idx ON edges (node1_id);
                    CREATE INDEX IF NOT EXISTS node2_idx ON edges (node2_id);
                    CREATE INDEX IF NOT EXISTS node_idx ON nodes (node_id);
                    ALTER TABLE edges ADD CONSTRAINT edges_node1_id_fkey 
                        FOREIGN KEY (node1_id) REFERENCES nodes (node_id);
                    ALTER TABLE edges ADD CONSTRAINT edges_node2_id_fkey 
                        FOREIGN KEY (node2_id) REFERENCES nodes (node_id);
                """)
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"Error during structure restoration: {str(e)}")

    # Summary
    print("\nImport summary:")
    print(f"Imported nodes: {total_nodes}")
    print(f"Imported edges: {total_edges}")
    print(f"Skipped lines: {skipped_lines}")

if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description='Import CSKG data to PostgreSQL')
    parser.add_argument("--tsv", required=True, help="C:\\Users\\szymo\\pythonDatabase\\cskg.tsv")
    parser.add_argument("--clean", action="store_true", help="Clear old data before import")
    parser.add_argument("--batch", type=int, default=50000, help="Batch size")
    args = parser.parse_args()
    
    try:
        import_data(
            args.tsv, 
            batch_size=args.batch,
            clean=args.clean
        )
    except Exception as e:
        print(f"Import failed: {str(e)}")
        sys.exit(1)
    
    print(f"\nTotal execution time: {time.time() - start:.2f}s")