//
// private_file.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Creation of the files that hold a kernel's signing key.

use std::fs::File;
use std::io;
use std::path::Path;

/// Create a file for writing that only the user the server runs as can read.
///
/// A kernel's connection file and registration file hold the key the kernel's
/// messages are signed with. They are written to the temporary directory,
/// which on many systems every user can list and read from. `File::create`
/// leaves a new file's permissions to the process's umask, and leaves the
/// permissions of a file that is already there as they are.
///
/// On Unix, a file already at the path is removed and the new one is created
/// with mode 0600 in the call that creates it, so that there is no moment at
/// which others can read it. If something appears at the path in between, the
/// creation fails; the key is not written into a file somebody else made.
///
/// On other systems the file is created as `File::create` creates it.
pub fn create<P: AsRef<Path>>(path: P) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
    }

    #[cfg(not(unix))]
    {
        File::create(path)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::connection_file::ConnectionFile;
    use crate::registration_file::RegistrationFile;
    use kallichore_api::models::ConnectionInfo;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    /// The permission bits of a file.
    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn a_new_file_is_readable_by_its_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.json");

        create(&path).unwrap().write_all(b"new").unwrap();

        assert_eq!(mode(&path), 0o600);
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn a_file_others_could_read_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.json");
        std::fs::write(&path, b"old, and longer than the new contents").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        create(&path).unwrap().write_all(b"new").unwrap();

        assert_eq!(mode(&path), 0o600);
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn a_symbolic_link_is_replaced_not_written_through() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("somebody-elses-file");
        std::fs::write(&target, b"untouched").unwrap();
        let path = dir.path().join("key.json");
        std::os::unix::fs::symlink(&target, &path).unwrap();

        create(&path).unwrap().write_all(b"new").unwrap();

        assert_eq!(std::fs::read(&target).unwrap(), b"untouched");
        assert!(std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_file());
        assert_eq!(mode(&path), 0o600);
    }

    #[test]
    fn a_connection_file_is_readable_by_its_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("connection.json");
        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let connection_file = ConnectionFile::from_info(ConnectionInfo {
            control_port: 1,
            shell_port: 2,
            stdin_port: 3,
            hb_port: 4,
            iopub_port: 5,
            signature_scheme: "hmac-sha256".to_string(),
            key: "the-key".to_string(),
            transport: "tcp".to_string(),
            ip: "127.0.0.1".to_string(),
        });
        connection_file.to_file(&path).unwrap();

        assert_eq!(mode(&path), 0o600);
        let written: ConnectionInfo =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(written.key, "the-key");
    }

    #[test]
    fn a_registration_file_is_readable_by_its_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registration.json");
        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let registration_file =
            RegistrationFile::new("127.0.0.1".to_string(), 1234, Some("the-key".to_string()));
        registration_file.to_file(&path).unwrap();

        assert_eq!(mode(&path), 0o600);
        let written: RegistrationFile =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(written.key.as_deref(), Some("the-key"));
    }
}
