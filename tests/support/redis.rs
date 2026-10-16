use super::{cleanup_result, unique_name};
use std::process::Command;

pub struct RedisNamespace {
    pub name: String,
    url: String,
}

impl RedisNamespace {
    pub fn new(url: &str, prefix: &str) -> Self {
        Self {
            name: unique_name(prefix),
            url: url.into(),
        }
    }
}

impl Drop for RedisNamespace {
    fn drop(&mut self) {
        let output = Command::new("redis-cli")
            .args([
                "-u",
                &self.url,
                "--scan",
                "--pattern",
                &format!("{}*:*", self.name),
            ])
            .output()
            .expect("redis-cli is installed");
        cleanup_result(output.status.success(), "scan isolated Redis namespace");
        for key in String::from_utf8_lossy(&output.stdout).lines() {
            let output = Command::new("redis-cli")
                .args(["-u", &self.url, "DEL", key])
                .output()
                .expect("redis-cli is installed");
            cleanup_result(output.status.success(), "delete isolated Redis key");
        }
    }
}
