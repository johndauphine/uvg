//! Dialect-neutral graph algorithms over the introspected schema.

/// Sort tables in topological order by FK dependencies (Kahn's algorithm).
/// Referenced tables come before referencing tables. Alphabetical tiebreak.
pub fn topo_sort_tables(tables: &[crate::schema::TableInfo]) -> Vec<&crate::schema::TableInfo> {
    use std::collections::{BTreeSet, HashMap};

    // Build name→index map and adjacency / in-degree structures
    let name_to_idx: HashMap<&str, usize> = tables
        .iter()
        .enumerate()
        .map(|(i, t)| (t.name.as_str(), i))
        .collect();

    let n = tables.len();
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n]; // ref_table → [referencing tables]

    for (i, table) in tables.iter().enumerate() {
        for constraint in &table.constraints {
            if constraint.constraint_type == crate::schema::ConstraintType::ForeignKey {
                if let Some(ref fk) = constraint.foreign_key {
                    if let Some(&ref_idx) = name_to_idx.get(fk.ref_table.as_str()) {
                        if ref_idx != i {
                            // self-references don't count
                            in_degree[i] += 1;
                            dependents[ref_idx].push(i);
                        }
                    }
                }
            }
        }
    }

    // Kahn's: start with nodes that have no incoming FK edges, sorted alphabetically
    let mut queue: BTreeSet<(String, usize)> = BTreeSet::new();
    for (i, &deg) in in_degree.iter().enumerate() {
        if deg == 0 {
            queue.insert((tables[i].name.clone(), i));
        }
    }

    let mut result: Vec<&crate::schema::TableInfo> = Vec::with_capacity(n);
    while let Some((_, idx)) = queue.iter().next().cloned() {
        queue.remove(&(tables[idx].name.clone(), idx));
        result.push(&tables[idx]);
        for &dep in &dependents[idx] {
            in_degree[dep] -= 1;
            if in_degree[dep] == 0 {
                queue.insert((tables[dep].name.clone(), dep));
            }
        }
    }

    // If there's a cycle, append remaining tables alphabetically
    if result.len() < n {
        let in_result: std::collections::HashSet<usize> = result
            .iter()
            .map(|t| name_to_idx[t.name.as_str()])
            .collect();
        let mut remaining: Vec<(usize, &str)> = (0..n)
            .filter(|i| !in_result.contains(i))
            .map(|i| (i, tables[i].name.as_str()))
            .collect();
        remaining.sort_by_key(|&(_, name)| name.to_string());
        for (i, _) in remaining {
            result.push(&tables[i]);
        }
    }

    result
}
