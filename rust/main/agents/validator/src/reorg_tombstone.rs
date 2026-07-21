//! Durable local fail-stop state for validator reorg detection.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use eyre::{bail, Context, Result};
use hyperlane_core::ReorgEvent;

const REORG_TOMBSTONE_SUFFIX: &str = ".reorg-fail-stop";
static REORG_TOMBSTONE_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Return the local fail-stop tombstone adjacent to the validator database.
pub(crate) fn path_for_database(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(REORG_TOMBSTONE_SUFFIX);
    PathBuf::from(path)
}

/// Refuse startup while any tombstone filesystem entry exists.
pub(crate) fn ensure_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => bail!(
            "validator reorg fail-stop tombstone exists at {path:?}; refusing to start until an operator investigates and explicitly removes it"
        ),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("checking validator reorg fail-stop tombstone at {path:?}")),
    }
}

fn durable_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn fsync_directory(parent: &Path) -> Result<()> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("fsyncing reorg tombstone directory {parent:?}"))
}

/// Create and fsync the local tombstone before any remote or diagnostic I/O.
pub(crate) fn persist(path: &Path, event: &ReorgEvent) -> Result<()> {
    // Submitter clones can observe the same reorg concurrently. Serialize the
    // create/write/fsync sequence so AlreadyExists cannot return while another
    // clone still has an unflushed marker.
    let _write_guard = REORG_TOMBSTONE_WRITE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| eyre::eyre!("reorg tombstone write lock is poisoned"))?;
    let parent = durable_parent(path);
    fs::create_dir_all(parent)
        .with_context(|| format!("creating reorg tombstone directory {parent:?}"))?;

    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            // Existence alone is the fail-stop signal, but every successful
            // call must establish its crash durability before returning.
            fs::File::open(path)
                .and_then(|existing| existing.sync_all())
                .with_context(|| format!("fsyncing existing reorg tombstone at {path:?}"))?;
            fsync_directory(parent)?;
            return Ok(());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("creating reorg tombstone at {path:?}"));
        }
    };
    let serialized = serde_json::to_vec_pretty(event).context("serializing reorg tombstone")?;
    file.write_all(&serialized)
        .with_context(|| format!("writing reorg tombstone at {path:?}"))?;
    file.sync_all()
        .with_context(|| format!("fsyncing reorg tombstone at {path:?}"))?;
    fsync_directory(parent)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperlane_core::{ReorgPeriod, H256};

    #[test]
    fn persisted_tombstone_blocks_restart_until_explicit_removal() {
        let directory = tempfile::tempdir().unwrap();
        let path = path_for_database(&directory.path().join("validator-db"));
        let event = ReorgEvent::new(
            H256::repeat_byte(1),
            H256::repeat_byte(2),
            3,
            4,
            ReorgPeriod::from_blocks(5),
        );

        persist(&path, &event).unwrap();
        let first_contents = std::fs::read(&path).unwrap();
        persist(&path, &event).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), first_contents);
        assert!(ensure_absent(&path)
            .unwrap_err()
            .to_string()
            .contains("refusing to start"));
        std::fs::remove_file(&path).unwrap();
        ensure_absent(&path).unwrap();
    }

    #[test]
    fn adjacent_relative_database_uses_current_directory_for_durability() {
        let path = path_for_database(Path::new("validator-db"));
        assert_eq!(path, Path::new("validator-db.reorg-fail-stop"));
        assert_eq!(durable_parent(&path), Path::new("."));
    }
}
