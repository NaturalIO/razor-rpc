//! Utilities for graceful restart

use std::{
    any::type_name,
    env,
    fs::OpenOptions,
    io::{Error, Write, stderr},
    os::fd::{AsRawFd, RawFd},
    path::{Path, PathBuf},
    process,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use close_fds::set_fds_cloexec;
use log::*;
use orb::prelude::*;
use signal_hook::iterator::Signals;

/// Write pid file for current process
pub fn write_pid_file(p: &Path) -> std::io::Result<()> {
    let mut f = OpenOptions::new().write(true).truncate(true).create(true).open(p)?;
    let pid = process::id();
    f.write_all(pid.to_string().as_bytes())?;
    f.sync_data()?;
    Ok(())
}

/// A server-side listener management to support graceful restart
///
/// All socket listener managed by `Graceful` is inherited to the new program after restart.
/// New connection goes to new program. Old connections continue to be served at old program.
pub struct GracefulServer {
    run_dir: String,
    prog_name: String,
    listen_fds: Vec<String>,
    recovered: bool,
    recover_listen_fds: Vec<RawFd>,
    restart_timeout: Duration,
    close_signals: Vec<i32>,
}

impl GracefulServer {
    #[inline]
    pub fn new(
        run_dir: String, prog_name: String, restart_timeout: Duration,
        close_signals: Vec<libc::c_int>,
    ) -> Self {
        let mut graceful = Self {
            run_dir,
            prog_name,
            listen_fds: Vec::new(),
            recovered: false,
            recover_listen_fds: Vec::new(),
            restart_timeout,
            close_signals,
        };
        graceful.check_recover();
        return graceful;
    }

    fn pid_file_path(&self) -> PathBuf {
        return Path::new(&self.run_dir).join(format!("{}.pid", self.prog_name)).to_path_buf();
    }

    /// Call after program is fork. Use a separate empty file to notify parent that child has started up.
    fn write_child_pid_file(&self) -> std::io::Result<()> {
        let file_path =
            Path::new(&self.run_dir).join(format!("{}_{}", self.prog_name, process::id()));
        write_pid_file(&file_path)
    }

    fn check_recover(&mut self) {
        if let Some(env_recover_fds_str) = env::var_os("_GRACEFUL_RESTART") {
            let fds: Vec<&str> = env_recover_fds_str.to_str().unwrap().split(',').collect();
            for fd_str in fds {
                let fd = fd_str.parse::<RawFd>().unwrap();
                self.recover_listen_fds.push(fd);
            }
            self.recovered = true;
        }
    }

    fn restart(&mut self) -> std::io::Result<()> {
        let mut args = std::env::args_os();
        let arg0 = args.next().unwrap();
        let mut cmd = Command::new(arg0);
        for arg in args {
            cmd.arg(arg);
        }

        let mut preserved_fds = Vec::new();
        for fd_str in &self.listen_fds {
            let fd = fd_str.parse::<RawFd>().unwrap();
            preserved_fds.push(fd);
        }

        let listen_fds_str = self.listen_fds.join(",");
        cmd.env("_GRACEFUL_RESTART", &listen_fds_str);
        // set FD_CLOEXEC to parnet's fds except listening fds.
        // listening fds need to be inherited to support graceful restart
        preserved_fds.sort();
        let min_fd = stderr().as_raw_fd() + 1;
        set_fds_cloexec(min_fd, &preserved_fds);

        // parent fork child process failed, parent do not exit
        let mut child = cmd.spawn()?;
        let child_pid = child.id();
        let child_pid_file_path =
            Path::new(&self.run_dir).join(format!("{}_{}", &self.prog_name, child_pid));
        let start_ts = Instant::now();

        while Instant::now().duration_since(start_ts) <= self.restart_timeout {
            match std::fs::File::open(&child_pid_file_path) {
                Ok(_) => {
                    // parent remove parent_child_sync_file failed but graceful restart success, parent still exit
                    let _ = std::fs::remove_file(&child_pid_file_path);
                    return Ok(());
                }
                Err(_) => {
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        match child.try_wait() {
            Ok(Some(_)) => {
                // graceful restart failed child is exited, parent do not exit
                return Err(Error::other("graceful restart failed, child exited"));
            }
            Ok(None) => {
                // graceful restart triggered but child is not ready, parent still exit
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    /// Initiate socket listener which supports graceful restart.
    /// We assume the program does not change addrs, always call with the same order.
    pub async fn new_listener<L>(&mut self, addr: &str) -> std::io::Result<L>
    where
        L: AsyncListener + AsRawFd,
    {
        // XXX What if order wrong?
        if !self.recover_listen_fds.is_empty() {
            let raw_fd = self.recover_listen_fds.remove(0);
            match unsafe { L::try_from_raw_fd(addr, raw_fd) } {
                Ok(listener) => {
                    self.listen_fds.push(raw_fd.to_string());
                    return Ok(listener);
                }
                Err(e) => {
                    error!(
                        "graceful: cannot convert {} from_raw_fd {}: {}, fallback",
                        type_name::<L>(),
                        raw_fd,
                        e
                    );
                }
            }
        } else {
            warn!("no recover_listen_fds found");
        }
        match L::bind(addr).await {
            Err(e) => {
                error!("graceful: failed to listen {} {}", type_name::<L>(), addr);
                return Err(e);
            }
            Ok(listener) => {
                let raw_fd: RawFd = listener.as_raw_fd();
                // should clear the FD_CLOEXEC to avoid auto close when exec new program
                unsafe {
                    libc::fcntl(raw_fd, libc::F_SETFD, 0);
                }
                self.listen_fds.push(raw_fd.to_string());
                return Ok(listener);
            }
        }
    }

    /// when `can_graceful_restart` == true, will perform graceful restart on signals received,
    /// otherwise just perform graceful exit
    pub fn ready<F: FnOnce()>(
        &mut self, exit_closure: F, restart_signal: Option<libc::c_int>,
    ) -> std::io::Result<()> {
        // write process_pid_file failed, process start failed and exit
        write_pid_file(self.pid_file_path().as_ref())?;
        if self.recovered {
            // child creat parent_child_sync_file failed, child still continue run
            let _ = self.write_child_pid_file();
        }
        let mut sigs = self.close_signals.clone();
        if let Some(_signal) = restart_signal
            && !sigs.contains(&_signal)
        {
            sigs.push(_signal);
        }
        let mut signals = Signals::new(&sigs).unwrap();
        for signal in signals.forever() {
            if let Some(_signal) = restart_signal
                && _signal == signal
            {
                // graceful restart
                match self.restart() {
                    Ok(_) => break,
                    Err(_) => continue,
                }
            }
            // graceful exit
            break;
        }
        exit_closure();
        return Ok(());
    }
}
