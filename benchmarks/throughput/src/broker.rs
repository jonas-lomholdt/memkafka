use std::{
    fs::{File, OpenOptions},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(25);
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

struct LoopbackPorts {
    kafka: TcpListener,
    schema_registry: TcpListener,
}

impl LoopbackPorts {
    fn kafka_addr(&self) -> SocketAddrV4 {
        loopback_address(&self.kafka)
    }

    fn schema_registry_addr(&self) -> SocketAddrV4 {
        loopback_address(&self.schema_registry)
    }
}

fn reserve_loopback_ports() -> Result<LoopbackPorts> {
    let kafka = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .context("reserve loopback Kafka port")?;
    let schema_registry = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .context("reserve loopback Schema Registry port")?;
    Ok(LoopbackPorts {
        kafka,
        schema_registry,
    })
}

fn loopback_address(listener: &TcpListener) -> SocketAddrV4 {
    match listener
        .local_addr()
        .expect("bound loopback listener has a local address")
    {
        SocketAddr::V4(address) => address,
        SocketAddr::V6(_) => unreachable!("listener was explicitly bound to IPv4 loopback"),
    }
}

pub struct BrokerGuard {
    child: Option<Child>,
    bootstrap_server: String,
    log_path: PathBuf,
    pid: u32,
    sampler_stop: Arc<AtomicBool>,
    sampler: Option<JoinHandle<u64>>,
    peak_rss_bytes: Option<u64>,
}

impl BrokerGuard {
    pub fn start(binary: &Path) -> Result<Self> {
        let ports = reserve_loopback_ports()?;
        let kafka_addr = ports.kafka_addr();
        let schema_registry_addr = ports.schema_registry_addr();
        let bootstrap_server = kafka_addr.to_string();
        let log_path = new_log_path()?;
        let log = open_log(&log_path)?;
        let stderr = log
            .try_clone()
            .with_context(|| format!("clone broker log {}", log_path.display()))?;

        let mut command = Command::new(binary);
        command
            .arg("--kafka-listen")
            .arg(&bootstrap_server)
            .arg("--kafka-advertised-address")
            .arg(&bootstrap_server)
            .arg("--schema-registry-listen")
            .arg(schema_registry_addr.to_string())
            .arg("--quiet")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));

        // The child cannot bind while these reservations are held. Drop them only
        // after the complete command is prepared, immediately before spawning.
        drop(ports);
        let child = command.spawn().with_context(|| {
            format!(
                "start broker binary {} (log: {})",
                binary.display(),
                log_path.display()
            )
        })?;
        let pid = child.id();
        let sampler_stop = Arc::new(AtomicBool::new(false));
        let sampler = spawn_rss_sampler(pid, Arc::clone(&sampler_stop));
        let mut guard = Self {
            child: Some(child),
            bootstrap_server,
            log_path,
            pid,
            sampler_stop,
            sampler: Some(sampler),
            peak_rss_bytes: None,
        };

        guard.wait_until_ready(kafka_addr)?;
        Ok(guard)
    }

    pub fn bootstrap_server(&self) -> &str {
        &self.bootstrap_server
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn peak_rss_bytes(&mut self) -> Result<u64> {
        if let Some(peak) = self.peak_rss_bytes {
            return Ok(peak);
        }
        self.sampler_stop.store(true, Ordering::Release);
        let sampler = self.sampler.take().context("RSS sampler is unavailable")?;
        let peak = sampler
            .join()
            .map_err(|_| anyhow::anyhow!("RSS sampler panicked for broker PID {}", self.pid))?;
        if peak == 0 {
            bail!(
                "no positive RSS sample captured for broker PID {} (log: {})",
                self.pid,
                self.log_path.display()
            );
        }
        self.peak_rss_bytes = Some(peak);
        Ok(peak)
    }

    pub fn stop(&mut self) -> Result<()> {
        self.stop_sampler();
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };

        if child
            .try_wait()
            .with_context(|| format!("inspect broker PID {} before shutdown", self.pid))?
            .is_none()
        {
            child.kill().with_context(|| {
                format!(
                    "terminate broker PID {} (log: {})",
                    self.pid,
                    self.log_path.display()
                )
            })?;
        }
        child.wait().with_context(|| {
            format!(
                "wait for broker PID {} to exit (log: {})",
                self.pid,
                self.log_path.display()
            )
        })?;
        self.child = None;
        Ok(())
    }

    fn wait_until_ready(&mut self, kafka_addr: SocketAddrV4) -> Result<()> {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if TcpStream::connect_timeout(&SocketAddr::V4(kafka_addr), READINESS_POLL_INTERVAL)
                .is_ok()
            {
                return Ok(());
            }

            if let Some(status) = self
                .child
                .as_mut()
                .context("broker child is unavailable during startup")?
                .try_wait()
                .with_context(|| format!("inspect broker PID {} during startup", self.pid))?
            {
                bail!(
                    "broker PID {} exited before readiness with {status} (log: {})",
                    self.pid,
                    self.log_path.display()
                );
            }
            if Instant::now() >= deadline {
                bail!(
                    "broker PID {} did not accept Kafka connections at {} within {:.0}s (log: {})",
                    self.pid,
                    self.bootstrap_server,
                    STARTUP_TIMEOUT.as_secs_f64(),
                    self.log_path.display()
                );
            }
            thread::sleep(READINESS_POLL_INTERVAL);
        }
    }

    fn stop_sampler(&mut self) {
        self.sampler_stop.store(true, Ordering::Release);
        if let Some(sampler) = self.sampler.take()
            && let Ok(peak) = sampler.join()
        {
            self.peak_rss_bytes = Some(peak);
        }
    }
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn spawn_rss_sampler(pid: u32, stop: Arc<AtomicBool>) -> JoinHandle<u64> {
    thread::spawn(move || {
        let pid = Pid::from_u32(pid);
        let mut system = System::new();
        let mut peak = 0;
        loop {
            system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
            if let Some(process) = system.process(pid) {
                peak = peak.max(process.memory());
            }
            if stop.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(RSS_SAMPLE_INTERVAL);
        }
        peak
    })
}

fn new_log_path() -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "memkafka-throughput-{}-{timestamp}.log",
        std::process::id()
    )))
}

fn open_log(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("create broker log {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};

    use super::reserve_loopback_ports;

    #[test]
    fn holds_two_distinct_loopback_ports_until_broker_spawn() {
        let ports = reserve_loopback_ports().unwrap();

        assert_ne!(ports.kafka_addr(), ports.schema_registry_addr());
        assert_eq!(ports.kafka_addr().ip(), &Ipv4Addr::LOCALHOST);
        assert_eq!(ports.schema_registry_addr().ip(), &Ipv4Addr::LOCALHOST);
        assert!(
            TcpListener::bind(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                ports.kafka_addr().port()
            ))
            .is_err()
        );
        assert!(
            TcpListener::bind(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                ports.schema_registry_addr().port()
            ))
            .is_err()
        );
    }
}
