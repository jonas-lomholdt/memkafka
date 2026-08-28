use std::{
    ffi::{OsStr, OsString},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const USAGE: &str = "usage: kafka_api_capabilities (--check|--update) <capability-manifest-path>";
static NEXT_TEMPORARY_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Check,
    Update,
}

#[derive(Debug, Eq, PartialEq)]
struct Arguments {
    mode: Mode,
    path: PathBuf,
}

fn parse_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Arguments, String> {
    let mut arguments = arguments.into_iter();
    let mode = match arguments.next().as_deref() {
        Some(mode) if mode == OsStr::new("--check") => Mode::Check,
        Some(mode) if mode == OsStr::new("--update") => Mode::Update,
        _ => return Err(USAGE.to_owned()),
    };
    let Some(path) = arguments.next() else {
        return Err(USAGE.to_owned());
    };
    if arguments.next().is_some() {
        return Err(USAGE.to_owned());
    }

    Ok(Arguments {
        mode,
        path: PathBuf::from(path),
    })
}

fn render_to_path(mode: Mode, path: &Path, rendered: &str) -> Result<(), String> {
    match mode {
        Mode::Check => {
            let existing = fs::read(path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            if existing == rendered.as_bytes() {
                Ok(())
            } else {
                Err(format!(
                    "{} does not match the generated Kafka API capability manifest",
                    path.display()
                ))
            }
        }
        Mode::Update => atomic_write(path, rendered.as_bytes()),
    }
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| format!("manifest path {} has no file name", path.display()))?;
    let (temporary_path, mut temporary_file) = loop {
        let sequence = NEXT_TEMPORARY_FILE.fetch_add(1, Ordering::Relaxed);
        let mut temporary_name = OsString::from(".");
        temporary_name.push(file_name);
        temporary_name.push(format!(".{}.{}.tmp", std::process::id(), sequence));
        let temporary_path = parent.join(temporary_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
        {
            Ok(file) => break (temporary_path, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!(
                    "failed to create sibling temporary file for {}: {error}",
                    path.display()
                ));
            }
        }
    };

    let write_result = temporary_file
        .write_all(contents)
        .and_then(|()| temporary_file.sync_all());
    drop(temporary_file);
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary_path);
        return Err(format!(
            "failed to write temporary manifest for {}: {error}",
            path.display()
        ));
    }
    if let Err(error) = fs::rename(&temporary_path, path) {
        let _ = fs::remove_file(&temporary_path);
        return Err(format!(
            "failed to replace {} atomically: {error}",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        path::PathBuf,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use super::{Mode, NEXT_TEMPORARY_FILE, USAGE, parse_arguments, render_to_path};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
    static FILE_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn temporary_directory() -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "memkafka-capability-example-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("create temporary test directory");
        directory
    }

    #[test]
    fn arguments_require_exactly_one_mode_and_one_path() {
        let check = parse_arguments([OsString::from("--check"), OsString::from("target.json")])
            .expect("parse check arguments");
        assert_eq!(check.mode, Mode::Check);
        assert_eq!(check.path, PathBuf::from("target.json"));

        let update = parse_arguments([OsString::from("--update"), OsString::from("target.json")])
            .expect("parse update arguments");
        assert_eq!(update.mode, Mode::Update);
        assert_eq!(update.path, PathBuf::from("target.json"));

        for invalid in [
            vec![],
            vec![OsString::from("--check")],
            vec![OsString::from("--unknown"), OsString::from("target.json")],
            vec![
                OsString::from("--check"),
                OsString::from("target.json"),
                OsString::from("extra"),
            ],
        ] {
            assert_eq!(parse_arguments(invalid), Err(USAGE.to_owned()));
        }
    }

    #[test]
    fn failed_check_preserves_exact_bytes_and_names_the_path() {
        let _guard = FILE_TEST_LOCK.lock().expect("lock file tests");
        let directory = temporary_directory();
        let path = directory.join("capabilities.json");
        fs::write(&path, b"stable\n").expect("write checked file");

        render_to_path(Mode::Check, &path, "stable\n").expect("matching bytes pass");
        let error = render_to_path(Mode::Check, &path, "stable").expect_err("mismatch must fail");

        assert_eq!(
            error,
            format!(
                "{} does not match the generated Kafka API capability manifest",
                path.display()
            )
        );
        assert_eq!(fs::read(&path).expect("read checked file"), b"stable\n");
        fs::remove_dir_all(directory).expect("remove temporary test directory");
    }

    #[test]
    fn update_atomically_replaces_the_destination_via_a_sibling_file() {
        let _guard = FILE_TEST_LOCK.lock().expect("lock file tests");
        let directory = temporary_directory();
        let path = directory.join("capabilities.json");
        fs::write(&path, b"old\n").expect("write old manifest");

        render_to_path(Mode::Update, &path, "new\n").expect("update manifest");

        assert_eq!(fs::read(&path).expect("read updated manifest"), b"new\n");
        assert_eq!(
            fs::read_dir(&directory)
                .expect("list manifest directory")
                .count(),
            1,
            "atomic update must not leave its sibling temporary file behind"
        );
        fs::remove_dir_all(directory).expect("remove temporary test directory");
    }

    #[test]
    fn update_retries_a_sibling_temporary_file_collision() {
        let _guard = FILE_TEST_LOCK.lock().expect("lock file tests");
        let directory = temporary_directory();
        let path = directory.join("capabilities.json");
        let collision = directory.join(format!(
            ".capabilities.json.{}.{}.tmp",
            std::process::id(),
            NEXT_TEMPORARY_FILE.load(Ordering::Relaxed)
        ));
        fs::write(&collision, b"owned by another writer\n").expect("create colliding sibling");

        render_to_path(Mode::Update, &path, "new\n").expect("retry colliding sibling");

        assert_eq!(fs::read(&path).expect("read updated manifest"), b"new\n");
        assert_eq!(
            fs::read(&collision).expect("read colliding sibling"),
            b"owned by another writer\n"
        );
        assert_eq!(
            fs::read_dir(&directory)
                .expect("list manifest directory")
                .count(),
            2
        );
        fs::remove_dir_all(directory).expect("remove temporary test directory");
    }

    #[test]
    fn failed_atomic_replace_preserves_destination_and_cleans_temporary_file() {
        let _guard = FILE_TEST_LOCK.lock().expect("lock file tests");
        let directory = temporary_directory();
        let path = directory.join("capabilities.json");
        fs::create_dir(&path).expect("create destination directory");
        fs::write(path.join("sentinel"), b"preserved\n").expect("write destination sentinel");

        let error =
            render_to_path(Mode::Update, &path, "new\n").expect_err("atomic replace must fail");

        assert!(
            error.starts_with(&format!("failed to replace {} atomically:", path.display())),
            "unexpected diagnostic: {error}"
        );
        assert_eq!(
            fs::read(path.join("sentinel")).expect("read destination sentinel"),
            b"preserved\n"
        );
        assert_eq!(
            fs::read_dir(&directory)
                .expect("list manifest directory")
                .count(),
            1,
            "failed update must remove its sibling temporary file"
        );
        fs::remove_dir_all(directory).expect("remove temporary test directory");
    }
}

fn main() {
    let arguments = match parse_arguments(std::env::args_os().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let manifest = match memkafka::kafka::capabilities::manifest_json() {
        Ok(manifest) => manifest,
        Err(error) => {
            eprintln!("failed to render Kafka API capability manifest: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = render_to_path(arguments.mode, &arguments.path, &manifest) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
