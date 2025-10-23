-- schema.sql
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    node_label TEXT
);

CREATE TABLE IF NOT EXISTS edges (
    edge_id INTEGER PRIMARY KEY,
    node1_id TEXT REFERENCES nodes(node_id) ON DELETE CASCADE,
    node2_id TEXT REFERENCES nodes(node_id) ON DELETE CASCADE,
    relation TEXT,
    relation_label TEXT
);

CREATE INDEX IF NOT EXISTS node1_idx ON edges (node1_id);
CREATE INDEX IF NOT EXISTS node2_idx ON edges (node2_id);
CREATE INDEX IF NOT EXISTS node_idx ON nodes (node_id);