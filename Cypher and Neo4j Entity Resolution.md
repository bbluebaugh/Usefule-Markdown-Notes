The Recommended Architecture: Asynchronous Batch Resolution

Because Kafka feeds data continuously, you should run your Neo4j Graph Data Science (GDS) and entity resolution steps as a **scheduled batch job** (e.g., nightly, weekly, or after large bulk imports) rather than trying to do it per Kafka message.

```
[ Python Clean ] ──► [ Kafka Topic ] ──► [ Cypher MERGE Ingest ] ──► [ Neo4j DB (Live) ]
                                                                             │
                                                                             ▼ (Scheduled Batch Job)
                                                                     ┌───────────────────────────┐
                                                                     │ PHASE 2: GDS RESOLUTION   │
                                                                     │ 1. Project dirty subgraph │
                                                                     │ 2. Find merger clusters   │
                                                                     │ 3. Resolve via APOC/Links │
                                                                     └───────────────────────────┘
```

---

Step-by-Step Implementation for Your Pipeline

Since you are dealing with **Parquet/CSV inputs** converted to JSON, your nodes likely end up in Neo4j with properties like `name` (cleaned string) and `company_id` (which might be messy due to mergers). Here is how to resolve them post-load.

1. Group Merged IDs via Weakly Connected Components (WCC)

When a corporate merger causes one company to assume the ID value of another, the graph topology will show two distinct name nodes pointing to the same identifier node. You can find these clusters using GDS.

First, project the relevant subset of your graph into memory:

```cypher
CALL gds.graph.project(
  'company_merger_graph',
  ['Company', 'Identifier'],
  {
    HAS_ID: { orientation: 'UNDIRECTED' }
  }
)
```

Next, run WCC to assign a unique cluster ID to every group of connected companies and identifiers:

```cypher
CALL gds.wcc.write('company_merger_graph', {
  writeProperty: 'mergerClusterId'
})
```

_Result:_ If `Walmart_Old` and `Walmart_New` are connected via a shared historical ID relationship anywhere in the database, they will now both share the exact same `mergerClusterId` integer.

2. Resolve Names via Jaro-Winkler or Levenshtein

For nodes that share a cluster ID (or for remaining nodes that don't have IDs but have highly similar names), use Neo4j APOC text distance functions to evaluate if they are actually the same entity.

You can run a query targeting nodes in the same cluster to find close matches:

```cypher
MATCH (c1:Company), (c2:Company)
WHERE c1.mergerClusterId = c2.mergerClusterId 
  AND id(c1) < id(c2)
WITH c1, c2, apoc.text.jaroWinklerDistance(c1.name, c2.name) AS similarity
WHERE similarity > 0.92
// Create a temporary relationship indicating they are duplicates
MERGE (c1)-[r:POTENTIAL_DUPLICATE]->(c2)
SET r.score = similarity
```

3. Consolidate the Graph (Handle the Duplicates)
## Step 1: Establish the Master-Alias Structure

Instead of changing how data enters the system, run a scheduled post-load batch script inside Neo4j. This script looks for duplicates (using the Graph Data Science WCC algorithm and APOC text distance metrics discussed earlier) and connects them to a single `:MasterCompany` node.

```
                  ┌──► (:Company {name: "WALMART INC"})
                  │
(:MasterCompany) ─┼──► (:Company {name: "WALMART INCORPORATED"})
                  │
                  └──► (:Company {name: "WALMART"})
```

Here is the Cypher script to dynamically generate these Master nodes and build the alias links:

```cypher
// 1. Find all pairs marked as duplicates by your resolution pipeline
MATCH (c1:Company)-[:POTENTIAL_DUPLICATE]->(c2:Company)

// 2. Pick a "Golden" name (e.g., the shortest name or the one with the most connections)
WITH c1, c2, 
     CASE WHEN size(c1.name) <= size(c2.name) THEN c1.name ELSE c2.name END AS goldenName

// 3. Create a unique Master node for this cluster
MERGE (m:MasterCompany {canonical_name: goldenName})

// 4. Link both duplicate variations to the Master node as aliases
MERGE (c1)-[:IS_ALIAS_FOR]->(m)
MERGE (c2)-[:IS_ALIAS_FOR]->(m)
```
## Step 3: How to Query Without Seeing Duplicates

Now that your data is structured this way, you can completely eliminate duplicates from your application's search results by **only querying the `:MasterCompany` label**.

### Scenario A: Global Search (Filtering Duplicates)

If a user searches for a company or you are generating a list, query the Master nodes directly. You will only ever get one result per true entity.

```cypher
MATCH (m:MasterCompany)
WHERE m.canonical_name CONTAINS "WALMART"
RETURN m.canonical_name AS CompanyName
```
### Scenario B: Finding Data Associated with Aliases

If you need to pull products, financial metrics, or data points that are scattered across the various duplicate `:Company` nodes, traverse the alias relationship in your query:

```cypher
MATCH (m:MasterCompany {canonical_name: "WALMART"})<-[:IS_ALIAS_FOR]-(alias:Company)-[:SELLS]->(p:Product)
RETURN m.canonical_name AS Company, collect(DISTINCT p.product_name) AS Products
```
_Why this works:_ Even if `WALMART INC` sells Product A, and `WALMART CORP` sells Product B, this query unifies them seamlessly into a single result under the name "WALMART".

---

Because your companies have critical directed relationships—like `(:Company)-[:HAS_CONTRACT]->(:Contract)`—the **Master-Alias (Parent-Child) pattern** is particularly powerful. It aggregates these scattered historical relationships under a single canonical entity without requiring you to manually rewrite, move, or duplicate thousands of relationship edges.

Here is the exact blueprint and Cypher strategy to build this post-ingest aggregation step.

---

## The Architecture: Virtual Relationship Aggregation

Instead of physically deleting the individual `:Company` nodes or migrating their edges (which risks missing data or breaking future weekly Kafka syncs), you will use the `:MasterCompany` node as an **aggregation umbrella**.

```
(:Contract A) ◄── [:HAS_CONTRACT] ── (:Company {name: "WALMART INC"}) ──┐
                                                                       ├── [:IS_ALIAS_FOR] ──► (:MasterCompany {canonical_name: "WALMART"})
(:Contract B) ◄── [:HAS_CONTRACT] ── (:Company {name: "WALMART CORP"}) ──┘
```

When you query the database, you simply look through the `:MasterCompany` node down into its aliases. Neo4j handles this traversal instantly, virtually aggregating all contracts, products, and financial histories.

---

### Step 1: The Post-Ingest Entity Resolution Script

Run this script once a week right after your Kafka ingestion finishes. This script uses APOC's text distance metrics to find highly similar company names and groups them under a single master node.
```cypher
// 1. Find pairs of companies with highly similar names that aren't already linked
MATCH (c1:Company), (c2:Company)
WHERE id(c1) < id(c2) 
  AND NOT (c1)-[:IS_ALIAS_FOR]->()
  AND NOT (c2)-[:IS_ALIAS_FOR]->()
  AND apoc.text.jaroWinklerDistance(c1.name, c2.name) > 0.92

// 2. Pick the shortest/cleanest name to represent the Master Entity
WITH c1, c2, 
     CASE WHEN size(c1.name) <= size(c2.name) THEN c1.name ELSE c2.name END AS goldenName

// 3. Create or match the unique MasterCompany node
MERGE (m:MasterCompany {canonical_name: goldenName})

// 4. Connect the dirty Kafka-managed nodes to the Master
MERGE (c1)-[:IS_ALIAS_FOR]->(m)
MERGE (c2)-[:IS_ALIAS_FOR]->(m);
```
#### Handling Remaining Solo Companies

For companies that are unique and do not have any duplicates, they still need a Master node so your application can query a uniform schema. Run this secondary cleanup step right after:

```cypher
// Find any company node that escaped the duplicate matching and give it a master node
MATCH (c:Company)
WHERE NOT (c)-[:IS_ALIAS_FOR]->(:MasterCompany)
MERGE (m:MasterCompany {canonical_name: c.name})
MERGE (c)-[:IS_ALIAS_FOR]->(m);
```
### Step 2: Querying the Aggregated Graph

Now that your weekly post-ingest script has mapped the graph, your application logic becomes incredibly clean. You can fetch a company and pull all of its aggregated contracts across all of its misspelled or merged aliases in a single query.

Example: Pulling All Contracts for a Company

```cypher
MATCH (m:MasterCompany)
WHERE m.canonical_name CONTAINS "WALMART"

// Traverse down to all aliases, and out to their respective contracts
MATCH (m)<-[:IS_ALIAS_FOR]-(alias:Company)-[:HAS_CONTRACT]->(contract:Contract)

// Return the clean name and a deduplicated list of all contract IDs/details
RETURN m.canonical_name AS CompanyName, 
       count(contract) AS TotalContracts,
       collect(DISTINCT contract.contract_id) AS AggregatedContractIDs
```
## Alternative: Physical Node Merging (If you absolutely cannot use Alias nodes)

If your architecture requirements strictly state that you **cannot** have extra `:MasterCompany` nodes and you must physically collapse everything into one node, you can use `apoc.refactor.mergeNodes`.

This function automatically takes all the relationship edges (like `[:HAS_CONTRACT]`) from the duplicate nodes and attaches them to the surviving "Golden" node.

```cypher
MATCH (c1:Company {name: "WALMART INC"}), (c2:Company {name: "WALMART CORP"})
WITH [c1, c2] AS nodes

// This will collapse c2 into c1, and move all of c2's contracts over to c1
CALL apoc.refactor.mergeNodes(nodes, {
  properties: "combine", // Merges properties into arrays if they differ
  mergeRels: true        // Combines identical relationships
}) 
YIELD node
RETURN node;
```