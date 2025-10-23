#!/usr/bin/env python3
"""
CSKG Database CLI Tool
A comprehensive command-line interface for querying and managing a CSKG (CommonSense Knowledge Graph) database.
Supports 18 different operations on nodes and edges in the knowledge graph.
"""

import psycopg2
import argparse
import time
from psycopg2 import Error

# Dictionary containing all SQL queries with their corresponding numbers
QUERIES = {
    # Query 1: Find all successors of a given node (nodes it points to)
    1: """
        SELECT DISTINCT e.node2_id, n.node_label, 
               STRING_AGG(DISTINCT e.relation, ', ' ORDER BY e.relation) AS relations,
               STRING_AGG(DISTINCT e.relation_label, ', ' ORDER BY e.relation_label) AS relation_labels
        FROM edges e
        JOIN nodes n ON e.node2_id = n.node_id
        WHERE e.node1_id = %s
        GROUP BY e.node2_id, n.node_label
        ORDER BY e.node2_id
    """,
    
    # Query 2: Count all successors of a given node
    2: "SELECT COUNT(*) FROM edges WHERE node1_id = %s",
    
    # Query 3: Find all predecessors of a given node (nodes that point to it)
    3: """
        SELECT 
        STRING_AGG(DISTINCT e.node1_id, ', ' ORDER BY e.node1_id) AS node_ids,  -- Agreguj wszystkie node_id
        n.node_label,
        STRING_AGG(DISTINCT e.relation, ', ' ORDER BY e.relation) AS relations,
        STRING_AGG(DISTINCT e.relation_label, ', ' ORDER BY e.relation_label) AS relation_labels
    FROM edges e
    JOIN nodes n ON e.node1_id = n.node_id
    WHERE e.node2_id = %s
    GROUP BY n.node_label  -- Grupowanie tylko po etykiecie
    ORDER BY n.node_label
    """,
    
    # Query 4: Count all predecessors of a given node
    4: "SELECT COUNT(*) FROM edges WHERE node2_id = %s",
    
    # Query 5: Find all neighbors of a given node (both successors and predecessors)
    5: """
        SELECT 
    STRING_AGG(DISTINCT e.node_id, ', ') AS nodes,  -- agreguj wszystkie node_id
    n.node_label,
    STRING_AGG(DISTINCT e.relation, ', ') AS relations,
    STRING_AGG(DISTINCT e.relation_label, ', ') AS relation_types
FROM (
    SELECT node2_id AS node_id, relation, relation_label FROM edges WHERE node1_id = %s
    UNION
    SELECT node1_id AS node_id, relation, relation_label FROM edges WHERE node2_id = %s
) e
JOIN nodes n ON e.node_id = n.node_id
GROUP BY n.node_label
    """,
    
    # Query 6: Count all unique neighbors of a given node
    6: """
        SELECT COUNT(*) FROM (
            SELECT node2_id FROM edges WHERE node1_id = %s
            UNION
            SELECT node1_id FROM edges WHERE node2_id = %s
        ) t
    """,
    
    # Query 7: Find successors of successors (2-hop forward connections)
    7: """
        WITH successors AS (
            SELECT node2_id FROM edges WHERE node1_id = %s
        )
        SELECT e.node2_id, n.node_label, e.relation_label
        FROM edges e
        JOIN nodes n ON e.node2_id = n.node_id
        WHERE e.node1_id IN (SELECT node2_id FROM successors)
    """,
    
    # Query 8: Find predecessors of predecessors (2-hop backward connections)
    8: """
        WITH predecessors AS (
            SELECT node1_id FROM edges WHERE node2_id = %s
        )
        SELECT e.node1_id, n.node_label, e.relation_label
        FROM edges e
        JOIN nodes n ON e.node1_id = n.node_id
        WHERE e.node2_id IN (SELECT node1_id FROM predecessors)
    """,
    
    # Query 9: Count all nodes in the database
    9: "SELECT COUNT(*) FROM nodes",
    
    # Query 10: Count nodes with no outgoing edges (sources)
    10: """
        SELECT COUNT(*) FROM (
            SELECT n.node_id 
            FROM nodes n
            WHERE NOT EXISTS (
                SELECT 1 FROM edges e WHERE e.node1_id = n.node_id
            )
        ) t
    """,
    
    # Query 11: Count nodes with no incoming edges (sinks)
    11: """
        SELECT COUNT(*) FROM (
            SELECT n.node_id 
            FROM nodes n
            WHERE NOT EXISTS (
                SELECT 1 FROM edges e WHERE e.node2_id = n.node_id
            )
        ) t
    """,
    
    # Query 12: Find the most connected node(s) (with highest degree)
    12: """WITH out_degrees AS (
          SELECT node1_id AS node_id, COUNT(DISTINCT node2_id) AS out_degree
          FROM edges
          GROUP BY node1_id
       )
       SELECT n.node_id, n.node_label, d.out_degree 
       FROM out_degrees d
       JOIN nodes n ON n.node_id = d.node_id
       WHERE d.out_degree = (SELECT MAX(out_degree) FROM out_degrees)
       ORDER BY n.node_id""",

    # Query 13: Count nodes with exactly one connection (degree = 1)
    13: """WITH all_neighbors AS (
              SELECT node1_id AS node_id FROM edges
              UNION ALL
              SELECT node2_id AS node_id FROM edges
           ),
           counts AS (
              SELECT node_id, COUNT(*) AS cnt FROM all_neighbors GROUP BY node_id
           )
           SELECT COUNT(*) FROM counts WHERE cnt = 1""",

    # Query 14: Rename a node (update all references in both nodes and edges)
    14: """
        BEGIN;
        
        INSERT INTO nodes (node_id, node_label) VALUES (%s, %s);
        
        UPDATE edges SET node1_id = %s WHERE node1_id = %s;
        UPDATE edges SET node2_id = %s WHERE node2_id = %s;
        
        DELETE FROM nodes WHERE node_id = %s;
        COMMIT;
    """,
    
    # Query 15: Find all "similar" nodes - those sharing a common parent or child by same edge type
    15: """
        WITH common_parents AS (
        SELECT 
            e2.node2_id AS similar_node, 
            'common_parent' AS similarity_type,
            STRING_AGG(DISTINCT e1.relation, ' | ' ORDER BY e1.relation) AS relation_types
        FROM edges e1
        JOIN edges e2 ON e1.node1_id = e2.node1_id AND e1.relation = e2.relation
        WHERE e1.node2_id = %s AND e2.node2_id != %s
        GROUP BY e2.node2_id
    ),
    common_children AS (
        SELECT 
            e2.node1_id AS similar_node, 
            'common_child' AS similarity_type,
            STRING_AGG(DISTINCT e1.relation, ' | ' ORDER BY e1.relation) AS relation_types
        FROM edges e1
        JOIN edges e2 ON e1.node2_id = e2.node2_id AND e1.relation = e2.relation
        WHERE e1.node1_id = %s AND e2.node1_id != %s
        GROUP BY e2.node1_id
    )
    SELECT 
        n.node_id,
        n.node_label,
        STRING_AGG(DISTINCT sim.similarity_type, ' | ' ORDER BY sim.similarity_type) AS similarity_types,
        STRING_AGG(DISTINCT sim.relation_types, ' | ') AS relations
    FROM (
        SELECT similar_node, similarity_type, relation_types FROM common_parents
        UNION ALL
        SELECT similar_node, similarity_type, relation_types FROM common_children
    ) sim
    JOIN nodes n ON sim.similar_node = n.node_id
    GROUP BY n.node_id, n.node_label
    ORDER BY n.node_label
    """,
    
    # Query 16: Find the shortest path between two nodes (undirected) and return the path
    16: """
        WITH RECURSIVE path_finder AS (
        SELECT %s::TEXT AS node_id, ARRAY[%s::TEXT] AS path, 0 AS depth
        UNION ALL
        SELECT 
            CASE WHEN e.node1_id = pf.node_id THEN e.node2_id ELSE e.node1_id END AS next_node,
            pf.path || CASE WHEN e.node1_id = pf.node_id THEN e.node2_id ELSE e.node1_id END,
            pf.depth + 1
        FROM edges e
        JOIN path_finder pf 
            ON e.node1_id = pf.node_id OR e.node2_id = pf.node_id
        WHERE 
            NOT (
                CASE WHEN e.node1_id = pf.node_id THEN e.node2_id ELSE e.node1_id END
            ) = ANY(pf.path)
            AND pf.depth < 6
            AND NOT %s = ANY(pf.path)  
    )
    SELECT path, depth
    FROM path_finder
    WHERE node_id = %s
    ORDER BY depth
    LIMIT 1
    """,
    
    # Query 17: Find all distant synonyms of a given node at specified distance
    17: """
        WITH RECURSIVE unique_synonym_paths AS (
    SELECT 
        node_id,
        distance,
        sign,
        path,
        
        ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY array_length(path, 1)) AS path_rank
    FROM (
        WITH RECURSIVE synonym_paths(node_id, distance, sign, path) AS (
           
            SELECT 
                CASE WHEN e.node1_id = %s THEN e.node2_id ELSE e.node1_id END,
                1,
                CASE WHEN e.relation = '/r/Antonym' THEN -1 ELSE 1 END,
                ARRAY[CASE WHEN e.node1_id = %s THEN e.node2_id ELSE e.node1_id END]
            FROM edges e
            WHERE (e.node1_id = %s OR e.node2_id = %s)
            AND e.relation IN ('/r/Synonym', '/r/Antonym')
            
            UNION ALL
            
            SELECT 
                CASE WHEN e.node1_id = sp.node_id THEN e.node2_id ELSE e.node1_id END,
                sp.distance + 1,
                sp.sign * (CASE WHEN e.relation = '/r/Antonym' THEN -1 ELSE 1 END),
                sp.path || CASE WHEN e.node1_id = sp.node_id THEN e.node2_id ELSE e.node1_id END
            FROM edges e
            JOIN synonym_paths sp ON (e.node1_id = sp.node_id OR e.node2_id = sp.node_id)
            WHERE 
                NOT (CASE WHEN e.node1_id = sp.node_id THEN e.node2_id ELSE e.node1_id END) = ANY(sp.path)
                AND sp.distance < %s
                AND e.relation IN ('/r/Synonym', '/r/Antonym')
        )
        SELECT * FROM synonym_paths
    ) all_paths
)
SELECT DISTINCT ON (n.node_id)
    n.node_id, 
    n.node_label,
    sp.distance,
    
    (SELECT STRING_AGG(n2.node_label, ' → ' ORDER BY idx) 
    FROM UNNEST(sp.path) WITH ORDINALITY AS t(node, idx)
    JOIN nodes n2 ON t.node = n2.node_id
    ) AS path_labels
    FROM unique_synonym_paths sp
    JOIN nodes n ON sp.node_id = n.node_id
    WHERE sp.distance = %s 
    AND sp.sign = 1
    AND sp.path_rank = 1  -- Tylko jedna ścieżka per węzeł
    ORDER BY n.node_id, array_length(sp.path, 1)
        """,
    
    # Query 18: Find all distant antonyms of a given node at specified distance
    18: """
        WITH RECURSIVE synonym_paths(node_id, distance, sign, path) AS (
    -- Base case
    SELECT 
        CASE WHEN e.node1_id = %s THEN e.node2_id ELSE e.node1_id END,
        1,
        CASE WHEN e.relation = '/r/Antonym' THEN -1 ELSE 1 END,
        ARRAY[CASE WHEN e.node1_id = %s THEN e.node2_id ELSE e.node1_id END]
    FROM edges e
    WHERE (e.node1_id = %s OR e.node2_id = %s)
    AND e.relation IN ('/r/Synonym', '/r/Antonym')
    
    UNION ALL
    
    -- Recursive case
    SELECT 
        CASE WHEN e.node1_id = sp.node_id THEN e.node2_id ELSE e.node1_id END,
        sp.distance + 1,
        sp.sign * (CASE WHEN e.relation = '/r/Antonym' THEN -1 ELSE 1 END),
        sp.path || CASE WHEN e.node1_id = sp.node_id THEN e.node2_id ELSE e.node1_id END
    FROM edges e
    JOIN synonym_paths sp ON 
        (e.node1_id = sp.node_id OR e.node2_id = sp.node_id)
    WHERE 
        NOT (CASE WHEN e.node1_id = sp.node_id THEN e.node2_id ELSE e.node1_id END) = ANY(sp.path)
        AND sp.distance < %s
        AND e.relation IN ('/r/Synonym', '/r/Antonym')
),
ranked_paths AS (
    SELECT 
        node_id,
        distance,
        path,
        ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY array_length(path, 1)) AS rn
    FROM synonym_paths
    WHERE sign = -1
)
SELECT DISTINCT n.node_id, n.node_label
FROM ranked_paths rp
JOIN nodes n ON rp.node_id = n.node_id
WHERE rp.distance = %s AND rp.rn = 1
ORDER BY n.node_id

    """
}


def run_query(goal, node_id=None, new_id=None, new_label=None, node2_id=None, distance=None):
    """
    Execute a database query based on the specified goal and parameters.
    
    Args:
        goal (int): The query number to execute (1-18)
        node_id (str): The primary node ID for the query
        new_id (str): New node ID for rename operation (query 14)
        new_label (str): New node label for rename operation (query 14)
        node2_id (str): Second node ID for path finding (query 16)
        distance (int): Distance parameter for synonym/antonym queries (17-18)
    """
    conn = None
    try:
        # Establish database connection
        conn = psycopg2.connect(
            dbname="Projects2025",
            user="postgres",
            password="postgres",
            host="localhost"
        )
        conn.autocommit = False
        cur = conn.cursor()

        start_time = time.time()  # Start timing

        # Handle different query types with their required parameters
        if goal == 14:
            if not new_id or not new_label:
                print("Error: For operation 14, provide both new_id and new_label")
                return
            
            try:
                # Check if new ID already exists
                cur.execute("SELECT 1 FROM nodes WHERE node_id = %s", (new_id,))
                if cur.fetchone():
                    print(f"Error: Node ID {new_id} already exists")
                    return
                
                # Check if source node exists
                cur.execute("SELECT 1 FROM nodes WHERE node_id = %s", (node_id,))
                if not cur.fetchone():
                    print(f"Error: Source node {node_id} does not exist")
                    return
                
                # Execute transaction
                cur.execute(QUERIES[14], (
                    new_id, new_label,    # INSERT new node
                    new_id, node_id,      # UPDATE edges.node1_id
                    new_id, node_id,      # UPDATE edges.node2_id
                    node_id               # DELETE old node
                ))
                conn.commit()
                execution_time = time.time() - start_time
                print(f"Successfully renamed node {node_id} to {new_id} with label '{new_label}'")
                print(f"Execution time: {execution_time:.4f} seconds")
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Error renaming node: {e.pgerror}")
            return

        elif goal == 15:
            # Find similar nodes (common parents or children)
            if not node_id:
                print("Error: For operation 15, provide node_id")
                return
            
            try:
                # Execute with node_id repeated for both parameters
                cur.execute(QUERIES[15], (node_id, node_id, node_id, node_id))
                results = cur.fetchall()
                execution_time = time.time() - start_time
                
                if not results:
                    print(f"No similar nodes found for node {node_id}")
                else:
                    print(f"Similar nodes for {node_id}:")
                    for row in results:
                        print(f"- Node: {row[0]} | Label: {row[1]} | Similarity Type: {row[2]} | Relation: {row[3]}")
                print(f"Execution time: {execution_time:.4f} seconds")
            except psycopg2.Error as e:
                print(f"Database error: {e.pgerror}")
            return
          
        elif goal == 16:
            if not node_id or not node2_id:
                print("Error: For operation 16, provide both node_id and node2_id")
                return

            from collections import deque

            print(f"Running BFS from {node_id} to {node2_id}...")

            # Parameters
            max_depth = 10
            important_relations = [
            '/r/RelatedTo', '/r/IsA', '/r/PartOf',
            '/r/HasA', '/r/UsedFor', '/r/CapableOf', '/r/AtLocation'
            ]

            visited = set()
            queue = deque()
            queue.append((node_id, [node_id]))

            found = False

            while queue:
                current, path = queue.popleft()

                if current == node2_id:
                    found = True
                    break

                if len(path) > max_depth:
                    continue

                if current in visited:
                    continue

                visited.add(current)

                # Get neighbors of current node ON DEMAND
                cur.execute("""
                    SELECT node2_id FROM edges 
                    WHERE node1_id = %s AND relation = ANY(%s)
                    UNION
                    SELECT node1_id FROM edges 
                    WHERE node2_id = %s AND relation = ANY(%s)
                """, (current, important_relations, current, important_relations))

                neighbors = [row[0] for row in cur.fetchall()]

                for neighbor in neighbors:
                    if neighbor not in visited and neighbor not in path:
                        queue.append((neighbor, path + [neighbor]))

            execution_time = time.time() - start_time
            # Result
            if found:
                print(f"Shortest path distance: {len(path) - 1}")
                print("Path nodes:")
                for node in path:
                    cur.execute("SELECT node_label FROM nodes WHERE node_id = %s", (node,))
                    label = cur.fetchone()
                    label = label[0] if label else "Unknown"
                    print(f"- {node} ({label})")
            else:
                print("No path found between the nodes")
            print(f"Execution time: {execution_time:.4f} seconds")
            return
                            
        elif goal in [17, 18]:
            # Distant synonyms/antonyms queries
            if not node_id or distance is None:
                print(f"Error: For operation {goal}, provide node_id and distance parameters")
                return
            
            # Prepare parameters (4x node_id + 2x distance)
            params = (node_id, node_id, node_id, node_id, distance, distance)
            
            try:
                cur.execute(QUERIES[goal], params)
                results = cur.fetchall()
                execution_time = time.time() - start_time
                
                if not results:
                    relation_type = "synonyms" if goal == 17 else "antonyms"
                    print(f"No distant {relation_type} found for node {node_id} at distance {distance}")
                else:
                    relation_type = "synonyms" if goal == 17 else "antonyms"
                    print(f"Distant {relation_type} of {node_id} at distance {distance}:")
                    for row in results:
                        if row[0] != node_id:  
                            print(f"- {row[0]}: {row[1]}")
                print(f"Execution time: {execution_time:.4f} seconds")
            except psycopg2.Error as e:
                print(f"Database error: {e.pgerror}")
            
            return
        elif goal == 12:
            # First get all nodes and their labels
            cur.execute("SELECT node_id, node_label FROM nodes")
            nodes = {row[0]: row[1] for row in cur.fetchall()}
            
            # Initialize degree dictionary
            degrees = {node_id: 0 for node_id in nodes}
            
            # Count neighbors from node1_id (outgoing)
            cur.execute("SELECT node1_id, COUNT(DISTINCT node2_id) FROM edges GROUP BY node1_id")
            for node1_id, count in cur.fetchall():
                if node1_id in degrees:
                    degrees[node1_id] += count
            
            # Count neighbors from node2_id (incoming)
            cur.execute("SELECT node2_id, COUNT(DISTINCT node1_id) FROM edges GROUP BY node2_id")
            for node2_id, count in cur.fetchall():
                if node2_id in degrees:
                    degrees[node2_id] += count
            
            # Find maximum degree
            max_degree = max(degrees.values())
            
            # Get all nodes with maximum degree
            most_connected = [(node_id, nodes[node_id], degree) 
                            for node_id, degree in degrees.items() 
                            if degree == max_degree]
            
            # Sort by node_id for consistent output
            most_connected.sort()
            
            execution_time = time.time() - start_time
            #print(f"Most connected nodes (degree = {max_degree}):")
            for node_id, label, degree in most_connected:
                print(f"- Node: {node_id} | Label: {label} ")
            print(f"Execution time: {execution_time:.4f} seconds")
            return
        
        elif goal in [5, 6]:
            # Queries that need node_id twice
            cur.execute(QUERIES[goal], (node_id, node_id))
        elif node_id:
            # Queries that need node_id once
            cur.execute(QUERIES[goal], (node_id,))
        else:
            # Queries with no parameters
            cur.execute(QUERIES[goal])
        
        execution_time = time.time() - start_time
        
        # Process and display results based on query type
        if goal in [1, 3, 5, 7, 8]:
            results = cur.fetchall()
            if not results:
                print("No results found")
            else:
                for row in results:
                    if goal == 5:
                        print(f"Node: {row[0]} | Label: {row[1]} | Relation: {row[2]} | Type: {row[3]}")
                    elif goal == 12:
                        print(f"Node: {row[0]} | Label: {row[1]} | Neighbor count: {row[2]}")
                    elif goal == 15:
                        print(f"Similar Node: {row[0]} | Label: {row[1]} | Relation: {row[2]} | Similarity Type: {row[3]}")
                    else:
                        print(f"Node: {row[0]} | Label: {row[1]} | Relation: {row[2]}")
        else:
            # Count queries
            count = cur.fetchone()[0]
            print(f"Count: {count}")
        
        print(f"Execution time: {execution_time:.4f} seconds")

    except psycopg2.Error as e:
        print(f"Database error: {e.pgerror}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def main():
    """Main function to parse arguments and execute queries"""
    parser = argparse.ArgumentParser(
        description="CSKG Database CLI Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("goal", type=int, help="Operation number (1-18)")
    parser.add_argument("--node_id", help="Node ID (required for operations 1-8,14-18)")
    parser.add_argument("--new_id", help="New node ID (required for operation 14)")
    parser.add_argument("--new_label", help="New node label (required for operation 14)")
    parser.add_argument("--node2_id", help="Second node ID (required for operation 16)")
    parser.add_argument("--distance", type=int, help="Distance parameter (required for operations 17-18)")
    
    args = parser.parse_args()
    
    # Validate input parameters
    if args.goal < 1 or args.goal > 18:
        print("Error: Operation must be between 1 and 18")
    elif args.goal in [1,2,3,4,5,6,7,8,14,15,17,18] and not args.node_id:
        print(f"Error: node_id required for operation {args.goal}")
    elif args.goal == 14 and (not args.new_id or not args.new_label):
        print("Error: Both new_id and new_label required for operation 14")
    elif args.goal == 16 and (not args.node_id or not args.node2_id):
        print("Error: Both node_id and node2_id required for operation 16")
    elif args.goal in [17, 18] and args.distance is None:
        print(f"Error: distance parameter required for operation {args.goal}")
    else:
        run_query(args.goal, args.node_id, args.new_id, args.new_label, args.node2_id, args.distance)


if __name__ == "__main__":
    main()