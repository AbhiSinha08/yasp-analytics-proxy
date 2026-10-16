use super::{cleanup_result, unique_name};
use std::{
    path::PathBuf,
    process::{Child, Output},
};

pub struct ChildGuard(Option<Child>);

impl ChildGuard {
    pub fn new(child: Child) -> Self {
        Self(Some(child))
    }
    pub fn id(&self) -> u32 {
        self.0.as_ref().unwrap().id()
    }
    pub fn child(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }
    pub fn wait_with_output(mut self) -> Output {
        self.0
            .take()
            .unwrap()
            .wait_with_output()
            .expect("test child exits")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            cleanup_result(child.wait().is_ok(), "reap test child process");
        }
    }
}

pub struct TempDirectory(pub PathBuf);

impl TempDirectory {
    pub fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(unique_name(prefix));
        std::fs::create_dir(&path).expect("temporary test directory is created");
        Self(path)
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        cleanup_result(
            std::fs::remove_dir_all(&self.0).is_ok(),
            "remove temporary test directory",
        );
    }
}
