use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmpdir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "uvg-init-test-{label}-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn command(root: &Path) -> InitCommand {
    InitCommand {
        migrations_dir: root.join("migrations"),
        // Override the profiles path so tests stay hermetic (no writes to the
        // real ~/.config/uvg) and don't depend on process-global env vars.
        config: Some(root.join("profiles.yaml")),
    }
}

#[test]
fn test_init_project_creates_expected_artifacts() {
    let root = tmpdir("create");
    let args = command(&root);
    let report = init_project(&args).unwrap();

    let config_path = args.config.clone().unwrap();
    assert_eq!(report.created.len(), 4);
    assert!(args.migrations_dir.is_dir());
    assert!(args.migrations_dir.join(BASELINE_FILENAME).is_file());
    assert!(args.migrations_dir.join(META_FILENAME).is_file());
    assert!(config_path.is_file());

    let baseline = fs::read_to_string(args.migrations_dir.join(BASELINE_FILENAME)).unwrap();
    assert!(baseline.contains("-- uvg revision: 00000000_000000"));
    assert!(baseline.contains("-- UP"));

    let meta = fs::read_to_string(args.migrations_dir.join(META_FILENAME)).unwrap();
    assert!(meta.contains("head: '00000000_000000'"));

    // The scaffolded config is a profiles.yaml in the loader's schema, not the
    // old inert TOML.
    let config = fs::read_to_string(&config_path).unwrap();
    assert!(config.contains("profiles:"));
    assert!(config.contains(&format!("{SAMPLE_PROFILE_NAME}:")));
    assert!(config.contains("source: postgresql://localhost/dev"));
    assert!(config.contains("target: postgresql://localhost/staging"));

    fs::remove_dir_all(&root).ok();
}

#[test]
fn test_init_project_is_idempotent_and_preserves_existing_files() {
    let root = tmpdir("idempotent");
    let args = command(&root);
    let config_path = args.config.clone().unwrap();
    init_project(&args).unwrap();
    fs::write(&config_path, "custom: true\n").unwrap();

    let report = init_project(&args).unwrap();
    assert!(report.created.is_empty());
    assert_eq!(report.existing.len(), 4);
    assert_eq!(fs::read_to_string(&config_path).unwrap(), "custom: true\n");

    fs::remove_dir_all(&root).ok();
}

#[test]
fn test_init_project_errors_when_migrations_path_is_file() {
    let root = tmpdir("file-conflict");
    let args = command(&root);
    fs::write(&args.migrations_dir, "not a directory").unwrap();

    let err = init_project(&args).unwrap_err();
    assert!(err.to_string().contains("non-directory"));

    fs::remove_dir_all(&root).ok();
}

#[test]
fn test_profiles_stub_is_valid_yaml_in_loader_schema() {
    // Parse the stub as generic YAML and assert the shape the loader expects:
    // a top-level `profiles` map with a `source`/`target`-bearing entry.
    // (A full round-trip through the real loader lives in profile_tests.rs.)
    let stub = profiles_yaml_stub();
    let value: serde_yaml::Value = serde_yaml::from_str(&stub).unwrap();
    let profile = &value["profiles"][SAMPLE_PROFILE_NAME];
    assert_eq!(
        profile["source"].as_str(),
        Some("postgresql://localhost/dev")
    );
    assert_eq!(
        profile["target"].as_str(),
        Some("postgresql://localhost/staging")
    );
}
