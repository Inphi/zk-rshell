use command::COMMAND_MAP;
use command::Command;
use regex::Regex;
use rustyline;
use rustyline::completion::Completer;
use std::cell::{Ref, RefCell};
use std::cmp::max;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use zookeeper::{Acl, CreateMode, Stat, Subscription, ZkError, ZkState, ZooKeeper};

use display;

#[derive(Debug)]
struct CommandArgs(Command, String);

static PROMPT: &'static str = ">";
static NOT_CONNECTED_MSG: &'static str = "not connected";

lazy_static! {
    static ref LINE_RE: Regex =
        Regex::new(r#"("(?:[^"\\]|\\.)*")|('(?:[^'\\]|\\.)*')|(\S+)"#).unwrap();
}

pub struct Shell {
    root: String,
    cwd: String,
    zk: Rc<RefCell<Option<ZooKeeper>>>,
    sub: Option<Subscription>,
    conn_state: Arc<AtomicUsize>,
    conn_timeout: Duration,
}

impl Shell {
    const DISCONNECTED: usize = 0;
    const CONNECTED: usize = 1;
    const CONNECTING: usize = 2;
    const CONNECTED_READ_ONLY: usize = 4;
    const AUTH_FAILED: usize = 8;

    pub fn new(conn_timeout: Duration) -> RefCell<Shell> {
        RefCell::new(Shell {
            root: String::from("/"),
            cwd: String::from("/"),
            zk: Rc::new(RefCell::new(None)),
            sub: None,
            conn_state: Arc::new(AtomicUsize::new(Shell::DISCONNECTED)),
            conn_timeout: conn_timeout,
        })
    }

    pub fn prompt(&self) -> String {
        let state = match self.conn_state.load(Ordering::SeqCst) {
            Shell::CONNECTED => String::from("(CONNECTED)"),
            Shell::DISCONNECTED => String::from("(DISCONNECTED)"),
            Shell::CONNECTING => String::from("(CONNECTING)"),
            Shell::CONNECTED_READ_ONLY => String::from("(CONNECTING_RO)"),
            Shell::AUTH_FAILED => String::from("(AUTH_FAILED)"),
            x => panic!("invalid state {}", x),
        };
        format!("{} {}{} ", state, self.cwd, PROMPT)
    }

    pub fn process(&mut self, line: &str) -> bool {
        let line = line.trim();
        if line.is_empty() {
            return true;
        }

        let mut quit = false;
        match parse_line(line) {
            Ok(cmdargs) => match cmdargs.0 {
                Command::Connect => self.connect(&cmdargs.1),
                Command::Create => self.create(&cmdargs.1),
                Command::Disconnect => self.disconnect(&cmdargs.1),
                Command::Cd => self.cd(cmdargs.1.as_ref()),
                Command::Ls => self.ls(cmdargs.1.as_ref()),
                Command::Get => self.get(cmdargs.1.as_ref()),
                Command::Rm => self.rm(cmdargs.1.as_ref()),
                Command::Stat => self.stat(cmdargs.1.as_ref()),
                Command::Set => self.set(cmdargs.1.as_ref()),
                Command::Pwd => self.pwd(cmdargs.1.as_ref()),
                Command::Exit => quit = true,
                Command::Help => self.help(cmdargs.1.as_ref()),
            },
            Err(e) => display::print(e.as_ref()),
        }
        !quit
    }

    // TODO: timeout
    pub fn init_connect(&mut self, quorum: &Vec<String>) -> Result<(), ZkError> {
        let hosts: String = quorum.iter().fold(String::new(), |acc, host| {
            if acc.is_empty() {
                host.clone()
            } else {
                acc + "," + host
            }
        });
        let zk = ZooKeeper::connect(&hosts, self.conn_timeout, |e| {
            println!("connect: {:?}", e); //debugme
        })?;

        self.conn_state.store(Shell::CONNECTED, Ordering::SeqCst);

        let st = self.conn_state.clone();
        self.sub = Some(zk.add_listener(move |e| Shell::on_zk_event(e, st.clone())));

        let zopt = Rc::get_mut(&mut self.zk).unwrap().get_mut();
        ::std::mem::swap(zopt, &mut Some(zk));
        Ok(())
    }

    fn connect(&mut self, args: &str) {
        if args.is_empty() {
            display::print("missing quorum");
            return;
        }

        let state = self.conn_state.load(Ordering::SeqCst);
        if state == Shell::CONNECTED || state == Shell::CONNECTING {
            self.disconnect("");
        };

        let quorum: Vec<String> = args.split(",").map(|x| x.to_string()).collect();
        match self.init_connect(&quorum) {
            Ok(_) => {}
            Err(e) => self.print_zk_error(e),
        };
    }

    fn disconnect(&mut self, _args: &str) {
        match self.zk.borrow_mut().take() {
            Some(z) => {
                assert!(self.sub.is_some());
                z.remove_listener(self.sub.unwrap());
                match z.close() {
                    Ok(_) => self.conn_state.store(Shell::DISCONNECTED, Ordering::SeqCst),
                    _ => {}
                }
            }
            None => {}
        }
    }

    fn cd(&mut self, args: &str) {
        let path = {
            let tokens = tokenize_line(args);
            if tokens.is_empty() {
                self.root.clone()
            } else {
                self.normalize_path(&tokens[0])
            }
        };

        if let Some(zk) = self.zk.borrow_mut().as_ref() {
            let exists = match zk.exists(&path, false) {
                Ok(stat) => stat.is_some(),
                Err(_) => false,
            };

            if exists {
                self.cwd = path;
            } else {
                display::print(format!("{} doesn't exist", path).as_ref());
            }
        } else {
            display::print(NOT_CONNECTED_MSG);
        }
    }

    fn set(&self, args: &str) {
        let tokens = tokenize_line(args);
        let path = &tokens[0];
        let path_normalized = self.normalize_path(path);
        let data = tokens[1].clone();
        let version = if tokens.len() > 2 {
            let vstr = &tokens[2];
            match vstr.parse::<i32>() {
                Ok(i) => Some(i),
                Err(_) => {
                    display::print(&format!("invalid version {}", vstr));
                    return;
                }
            }
        } else {
            None
        };

        self.run_zk_op(|zk| {
            let res = zk.set_data(&path_normalized, data.into_bytes(), version);
            match res {
                Ok(_) => {}
                Err(e) => display_zk_error(path, &e),
            };
        });
    }

    fn create(&self, args: &str) {
        let tokens = tokenize_line(args);
        if tokens.len() < 2 {
            display::print("create <path> <value>");
            return;
        }

        let path = &tokens[0];
        let path_normalized = self.normalize_path(path);
        let data = tokens[1].clone();
        let ephemeral = tokens
            .get(2)
            .map_or(false, |v| v.parse::<bool>().unwrap_or(false));
        let sequence = tokens
            .get(3)
            .map_or(false, |v| v.parse::<bool>().unwrap_or(false));
        // TODO(*): recursive
        /*
        let _recursive = tokens
            .get(4)
            .map_or(false, |v| v.parse::<bool>().unwrap_or(false));
            */

        let mode = if ephemeral && sequence {
            CreateMode::EphemeralSequential
        } else if !ephemeral && sequence {
            CreateMode::PersistentSequential
        } else if ephemeral {
            CreateMode::Ephemeral
        } else {
            CreateMode::Persistent
        };

        self.run_zk_op(|zk| {
            let res = zk.create(
                &path_normalized,
                data.into_bytes(),
                Acl::open_unsafe().clone(),
                mode,
            );
            match res {
                Ok(_) => {}
                Err(e) => display_zk_error(path, &e),
            }
        });
    }

    fn ls(&self, args: &str) {
        let path = if args.len() == 0 {
            self.cwd.clone()
        } else {
            self.normalize_path(args)
        };

        self.run_zk_op(|zk| {
            let res = zk.get_children(&path, false);
            match res {
                Ok(paths) => {
                    for path in paths {
                        display::print(path.as_ref());
                    }
                }
                Err(e) => {
                    self.print_zk_error(e);
                }
            }
        });
    }

    fn get(&self, args: &str) {
        self.do_data_stat(args, |x| display::print(&String::from_utf8_lossy(&x.0)));
    }

    fn rm(&self, args: &str) {
        let path = self.normalize_path(args);

        self.run_zk_op(|zk| {
            let res = zk.delete(&path, None);
            match res {
                Ok(_) => {}
                Err(e) => {
                    self.print_zk_error(e);
                }
            }
        });
    }

    fn stat(&self, args: &str) {
        self.do_data_stat(args, |x| display::print(format!("{:#?}", x.1).as_ref()));
    }

    fn do_data_stat(&self, args: &str, f: fn((Vec<u8>, Stat))) {
        let path = self.normalize_path(args);

        self.run_zk_op(|zk| {
            let res = zk.get_data(&path, false);
            match res {
                Ok(data_stat) => {
                    f(data_stat);
                }
                Err(e) => {
                    self.print_zk_error(e);
                }
            }
        });
    }

    fn pwd(&self, _args: &str) {
        display::print(&self.cwd);
    }

    fn help(&self, _args: &str) {
        unimplemented!();
    }

    fn print_zk_error(&self, err: ZkError) {
        display::print(format!("{}", err).as_ref());
    }

    fn on_zk_event(zk: ZkState, conn_state: Arc<AtomicUsize>) {
        //println!("onevent: {:?}", zk); // debugme
        match zk {
            ZkState::Connected => {
                conn_state.store(Shell::CONNECTED, Ordering::SeqCst);
            }
            ZkState::Connecting => {
                conn_state.store(Shell::CONNECTING, Ordering::SeqCst);
            }
            ZkState::AuthFailed => {
                conn_state.store(Shell::AUTH_FAILED, Ordering::SeqCst);
            }
            ZkState::ConnectedReadOnly => {
                conn_state.store(Shell::CONNECTED_READ_ONLY, Ordering::SeqCst);
            }
            _ => {
                conn_state.store(Shell::DISCONNECTED, Ordering::SeqCst);
            }
        }
    }

    pub fn get_completer(this: &RefCell<Self>) -> ShellCompleter {
        ShellCompleter { sh: this }
    }

    // TODO(inphi): dot globbing
    fn normalize_path(&self, path: &str) -> String {
        let mut path = if path.starts_with("/") {
            path.to_string()
        } else if path == ".." {
            let path = Path::new(&self.cwd);
            path.parent()
                .map_or("/".to_string(), |p| p.to_str().unwrap().to_string())
        } else {
            let path = Path::new(&self.cwd).join(path);
            path.to_str().unwrap().to_string()
        };
        if !is_root(&path) && path.ends_with("/") {
            path = path[0..path.len() - 1].to_string();
        }
        path
    }

    fn run_zk_op<F>(&self, f: F)
    where
        F: FnOnce(&ZooKeeper),
    {
        if let Some(zk) = self.zk.borrow_mut().as_ref() {
            f(zk);
        } else {
            display::print(NOT_CONNECTED_MSG);
        }
    }
}

fn tokenize_line(args: &str) -> Vec<String> {
    LINE_RE
        .captures_iter(args)
        .map(|c| {
            let mut token = c.get(0).unwrap().as_str();
            if token.starts_with("'") || token.starts_with("\"") {
                token = &token[1..]
            }
            if token.ends_with("'") || token.ends_with("\"") {
                token = &token[0..token.len() - 1]
            }
            token.to_string()
        })
        .collect::<Vec<_>>()
}

fn display_zk_error(path: &str, err: &ZkError) {
    match err {
        ZkError::NoNode => {
            display::print(&format!("missing path in {} (try recursive)", path));
        }
        ZkError::NoChildrenForEphemerals => {
            display::print("ephemeral znodes cannot have children");
        }
        ZkError::InvalidACL => {
            display::print("invalid acl");
        }
        ZkError::NodeExists => {
            display::print(&format!("Path {} already exists", path));
        }
        e => {
            display::print(&format!("internal zk error: {:?}", e));
        }
    }
}

fn parse_line(line: &str) -> Result<(CommandArgs), String> {
    let pos = line.find(char::is_whitespace);
    let (cmdstr, args) = match pos {
        Some(p) => (&line[..p], &line[p + 1..]),
        None => (line, ""),
    };

    let cmd = COMMAND_MAP.get(cmdstr);

    match cmd {
        Some(c) => Ok(CommandArgs(c.clone(), args.to_string())),
        None => Err(format!("unknown command: {}", cmdstr).to_string()),
    }
}

pub struct ShellCompleter<'a> {
    sh: &'a RefCell<Shell>,
}

impl<'a> Completer for ShellCompleter<'a> {
    fn complete(&self, line: &str, pos: usize) -> rustyline::Result<(usize, Vec<String>)> {
        let args = Self::tokenize_line(line);

        let last_pos_in_char = if pos >= line.len() {
            max(0, line.len() as isize - 1) as usize
        } else {
            pos
        };
        if args.is_empty()
            || (args.len() <= 1 && !(line.as_bytes()[last_pos_in_char] as char).is_whitespace())
        {
            Ok((
                0,
                self.cmd_candidates(args.first().map_or(&"".to_string(), |s| &s.1)),
            ))
        } else if args.len() >= 1 {
            let (cmd, path) = (
                &args[0].1,
                &args.get(1).map_or("".to_string(), |s| s.1.clone()),
            );

            let path_pos = args.get(1).map_or(0, |s| s.0);

            // TODO(*): disable for certain commands (like connect)
            if COMMAND_MAP.contains_key(cmd as &str) {
                let candidates = self.path_candidates(path);
                Ok((candidates.0 + path_pos, candidates.1))
            } else {
                Ok((0, vec![]))
            }
        } else {
            Ok((0, vec![]))
        }
    }
}

impl<'a> ShellCompleter<'a> {
    fn cmd_candidates(&self, cmd_fragment: &str) -> Vec<String> {
        let mut candidates = Vec::<String>::new();
        for (s, _) in &*COMMAND_MAP {
            if s.starts_with(cmd_fragment) {
                candidates.push(s.to_string());
            }
        }
        candidates.sort();
        candidates
    }

    // returns the position to overwrite the path for completion candidates
    fn path_candidates(&self, path: &str) -> (usize, Vec<String>) {
        let mut candidates = Vec::<String>::new();

        let query_path = if path.is_empty() {
            self.sh.borrow().cwd.clone()
        } else if is_root(path) {
            path.to_string()
        } else if path.ends_with("/") {
            let path = path[0..path.len() - 1].to_string();
            if path.starts_with("/") {
                path.to_string()
            } else {
                (Path::new(&self.sh.borrow().cwd).join(path))
                    .to_str()
                    .unwrap()
                    .to_string()
            }
        } else {
            let query_path = if path.starts_with("/") {
                path.to_string()
            } else {
                (Path::new(&self.sh.borrow().cwd).join(path))
                    .to_str()
                    .unwrap()
                    .to_string()
            };

            let exists = Self::do_zk(Ref::clone(&self.sh.borrow()), |z| {
                z.exists(&query_path, false)
            });

            let (path_exists, has_children) = if let Some(e) = exists {
                if let Ok(stat) = e {
                    stat.map_or((false, false), |s| (true, s.num_children > 0))
                } else {
                    (false, false)
                }
            } else {
                (false, false)
            };

            if path_exists {
                if has_children {
                    return (0, vec![path.to_string() + "/"]);
                } else {
                    return (0, vec![path.to_string()]);
                }
            } else {
                let path = Path::new(&query_path);
                let parent = path.parent().unwrap();
                parent.to_str().unwrap().to_string()
            }
        };

        let basename = {
            let path_components = Path::new(&path).components();
            path_components.rev().next().map_or("".to_string(), |x| {
                Path::new(&x).to_str().unwrap().to_string()
            })
        };

        Self::do_zk(Ref::clone(&self.sh.borrow()), |zk| {
            let res = zk.get_children(&query_path, false);
            match res {
                Ok(children) => {
                    for child in children {
                        if child.starts_with(&basename) || is_root(&basename) || path.ends_with("/")
                        {
                            candidates.push(child);
                        }
                    }
                }
                _ => {
                    //println!("nope");
                }
            }
        });

        let last_slash = path.rfind("/").map_or(-1, |p| p as isize);

        ((last_slash + 1) as usize, candidates)
    }

    fn do_zk<F, R>(sh: Ref<Shell>, mut f: F) -> Option<R>
    where
        F: FnMut(&ZooKeeper) -> R,
    {
        let zk_ref = Ref::map(sh, |sh| &sh.zk);
        if let Some(zk) = zk_ref.borrow().as_ref() {
            return Some(f(zk));
        };
        None
    }

    fn tokenize_line(line: &str) -> Vec<(usize, String)> {
        let mut tokens: Vec<(usize, String)> = Vec::new();

        let mut pos: usize = 0;
        let mut non_ws = String::new();
        for c in line.chars() {
            // new word
            if c.is_whitespace() {
                // lstrip
                if !non_ws.is_empty() {
                    tokens.push((pos - non_ws.len(), non_ws.clone()));
                    non_ws.clear();
                }
            } else {
                non_ws.push(c);
            }
            pos += 1;
        }

        if line.chars().last().map_or(false, |c| !c.is_whitespace()) {
            tokens.push((line.len() - non_ws.len(), non_ws.clone()))
        }
        tokens
    }
}

fn is_root(s: &str) -> bool {
    s == "/"
}

#[cfg(test)]
mod tests {
    #[test]
    fn check_completion_tokenizer() {
        use super::*;

        let check = |a: Vec<(u32, &str)>, line: &str| {
            let y = ShellCompleter::tokenize_line(line);
            let x = a.iter()
                .map(|x| (x.0 as usize, String::from(x.1)))
                .collect::<Vec<(usize, String)>>();
            assert_eq!(x, y);
        };

        check(vec![(0, "a"), (2, "b")], "a b");
        check(vec![(1, "a")], " a");
        check(vec![(1, "a")], "\ta");
        check(vec![(0, "a")], "a ");
        check(vec![(1, "a"), (3, "b")], " a b ");
    }

    #[test]
    fn check_completion() {
        use super::*;

        let shell = Shell::new(Duration::from_secs(4));
        let hosts = vec!["localhost:2181".to_string()];
        let _ = shell.borrow_mut().init_connect(&hosts);
        shell.borrow_mut().create("foo ''");
        shell.borrow_mut().create("foo2 ''");
        let completer = Shell::get_completer(&shell);

        let check = |mut a: Vec<&str>, mut b: Vec<String>| {
            a.sort();
            b.sort();
            assert_eq!(a, b);
        };

        let mut s = "ls ";
        let mut val = completer.complete(s, s.len()).unwrap();
        assert_eq!(val.0, 3);
        check(vec!["foo", "foo2", "zookeeper"], val.1);

        s = "ls /";
        val = completer.complete(s, s.len()).unwrap();
        assert_eq!(val.0, 4);
        check(vec!["foo", "foo2", "zookeeper"], val.1);

        s = "ls /f";
        val = completer.complete(s, s.len()).unwrap();
        assert_eq!(val.0, 4);
        check(vec!["foo", "foo2"], val.1);

        s = "ls /fo";
        val = completer.complete(s, s.len()).unwrap();
        assert_eq!(val.0, 4);
        check(vec!["foo", "foo2"], val.1);

        s = "ls /foo";
        val = completer.complete(s, s.len()).unwrap();
        assert_eq!(val.0, 4);
        check(vec!["foo/"], val.1);
    }
}
