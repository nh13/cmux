//! SCRATCH measurement (never merge; agent split-burst): 4 splits sent at
//! once on ONE connection (as the app sends Cmd-D), 12 rounds. Prints each
//! round's answer times and the per-mark step times.

use super::super::*;
use std::io::{BufRead as _, Write as _};
use std::time::Instant;

const ROUNDS: usize = 12;
const BURST: usize = 4;

fn key(prefix: &str, n: u64) -> String {
    format!("{prefix}{:032x}", 0x5c00_0000_0000_0000_0000_0000_0000_0000u128 + u128::from(n))
}

fn terminal_key(n: u64) -> String {
    format!("5c000000000040008000{n:012x}")
}

fn pane_of_workspace(harness: &RecoveryHarness, name: &str) -> u64 {
    let tree = request(&harness.socket, serde_json::json!({"id": 9_100, "cmd": "list-workspaces"}));
    tree["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"].as_str() == Some(name))
        .and_then(|w| w["screens"][0]["panes"][0]["id"].as_u64())
        .unwrap_or_else(|| panic!("no pane for {name}: {tree}"))
}

#[test]
fn scratch_split_burst_one_connection_measure() {
    let mut harness = RecoveryHarness::start_unstarted("split-burst-1c");
    let spans_path = harness.dir.join("spans.jsonl");
    harness.extra_env.push(("CMUX_TUI_DEBUG_SPANS".into(), spans_path.display().to_string()));
    harness.restart();
    let stream = transport::connect(&harness.socket).unwrap();
    let mut writer = stream.try_clone_box().unwrap();
    let mut reader = BufReader::new(stream);
    let mut rounds = Vec::new();
    let mut all = Vec::new();
    let mut spreads = Vec::new();
    let mut lasts = Vec::new();
    let mut in_order = true;
    for round in 0..ROUNDS {
        let name = format!("burst1c-{round}");
        run_cat_workspace(&harness.socket, 100 + round, &name);
        let pane = pane_of_workspace(&harness, &name);
        std::thread::sleep(Duration::from_millis(300));
        let mut lines = String::new();
        let ids = (0..BURST).map(|n| format!("c{}", round * BURST + n)).collect::<Vec<_>>();
        for (n, id) in ids.iter().enumerate() {
            let k = (round * BURST + n) as u64;
            lines.push_str(
                &serde_json::json!({"id": id, "cmd": "split", "pane": pane,
                    "dir": "down", "cols": 80, "rows": 12,
                    "pane_id": key("pane_", k), "tab_id": key("tab_", k),
                    "terminal_id": terminal_key(k)})
                .to_string(),
            );
            lines.push('\n');
        }
        let start = Instant::now();
        writer.write_all(lines.as_bytes()).unwrap();
        writer.flush().unwrap();
        let mut seen = Vec::new();
        while seen.len() < BURST {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0, "connection closed");
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            let Some(id) = value["id"].as_str() else { continue };
            if ids.iter().any(|candidate| candidate == id) {
                assert_eq!(value["ok"], serde_json::json!(true), "{value}");
                seen.push((id.to_string(), start.elapsed().as_micros() as f64 / 1000.0));
            }
        }
        in_order &= seen.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>() == ids;
        let ends = seen.iter().map(|(_, at)| *at).collect::<Vec<_>>();
        let spread = ends[BURST - 1] - ends[0];
        spreads.push(spread);
        lasts.push(ends[BURST - 1]);
        all.extend(ends.iter().copied());
        rounds.push(serde_json::json!({"answer_at_ms": ends, "spread_ms": spread}));
        std::thread::sleep(Duration::from_millis(300));
    }
    let pct = |values: &mut Vec<f64>, p: f64| {
        values.sort_by(f64::total_cmp);
        values[((values.len() as f64 - 1.0) * p).round() as usize]
    };
    let mut warm_spreads = spreads[1..].to_vec();
    let mut warm_all = all[BURST..].to_vec();
    let mut warm_lasts = lasts[1..].to_vec();
    std::thread::sleep(Duration::from_millis(500));
    let spans = fs::read_to_string(&spans_path).unwrap_or_default();
    let mut steps: std::collections::BTreeMap<String, Vec<f64>> = Default::default();
    let mut writes = 0;
    for line in spans.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else { continue };
        let label = value["label"].as_str().unwrap_or("");
        if label.ends_with(".follow") || label == "host" {
            continue;
        }
        let mut previous = 0.0;
        for mark in value["marks"].as_array().into_iter().flatten() {
            let name = mark[0].as_str().unwrap_or("?").to_string();
            let at = mark[1].as_f64().unwrap_or(0.0) / 1000.0;
            if name == "projection.writer_committed" {
                writes += 1;
            }
            steps.entry(format!("{label}:{name}")).or_default().push(at - previous);
            previous = at;
        }
    }
    let step_summary = steps
        .into_iter()
        .filter(|(name, _)| !name.contains("lock.wait"))
        .map(|(name, mut gaps)| {
            let n = gaps.len();
            (
                name,
                serde_json::json!({"n": n, "p50": pct(&mut gaps, 0.5), "max": pct(&mut gaps, 1.0)}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    panic!(
        "SPLIT-BURST-RESULT {} STEPS traced=0 {}",
        serde_json::json!({
            "rounds": rounds,
            "replies_in_send_order": in_order,
            "projection_writes": writes,
            "spread_p50_ms": pct(&mut warm_spreads, 0.5),
            "spread_max_ms": pct(&mut warm_spreads, 1.0),
            "last_p50_ms": pct(&mut warm_lasts, 0.5),
            "last_max_ms": pct(&mut warm_lasts, 1.0),
            "latency_p50_ms": pct(&mut warm_all, 0.5),
            "latency_p95_ms": pct(&mut warm_all, 0.95),
            "latency_max_ms": pct(&mut warm_all, 1.0),
        }),
        serde_json::Value::Object(step_summary)
    );
}
