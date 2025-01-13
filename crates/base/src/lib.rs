use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use once_cell::sync::Lazy;

pub const PROJECT_VERSION: &str = env!("CARGO_PKG_VERSION");

pub static PROJECT_SEMVER: Lazy<semver::Version> = Lazy::new(|| {
    semver::Version::parse(PROJECT_VERSION).unwrap_or(semver::Version {
        major: 0,
        minor: 0,
        patch: 0,
        pre: semver::Prerelease::EMPTY,
        build: semver::BuildMetadata::EMPTY,
    })
});

pub const PROJECT_NAME: &str = "griphook";
pub const CEX_CONFIG_FILE_NAME: &str = "cexflow.yaml";

pub static PROJECT_CONFIG_DIR: Lazy<PathBuf> = Lazy::new(|| {
    ProjectDirs::from("", "", PROJECT_NAME)
        .expect("Failed to create `ProjectDirs` path")
        .config_dir()
        .to_path_buf()
});

#[must_use]
pub fn fallback_project_config_directories() -> Vec<PathBuf> {
    let Some(user_dirs) = directories::UserDirs::new() else {
        return Vec::new();
    };
    let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::new());
    vec![
        [user_dirs.home_dir(), (Path::new(".config")), (Path::new(PROJECT_NAME))].iter().collect(),
        [user_dirs.home_dir(), (Path::new(&format!(".{PROJECT_NAME}")))].iter().collect(),
        [&Path::new("/"), &Path::new("etc"), &Path::new(PROJECT_NAME)].iter().collect(),
        current_dir.iter().collect(),
    ]
}
