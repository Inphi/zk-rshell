#[macro_use]
extern crate clap;
extern crate regex;
extern crate rustyline;
extern crate zookeeper;

#[macro_use]
extern crate lazy_static;

mod command;
mod display;
mod shell;

use clap::{App, AppSettings, Arg};
use rustyline::error::ReadlineError;
use rustyline::{CompletionType, Config, EditMode, Editor};
use shell::Shell;
use std::env::home_dir;
use std::time::Duration;

fn main() {
    let matches = App::new("zk-rsh")
        .about("zookeeper shell")
        .version("1.0")
        .setting(AppSettings::DontCollapseArgsInUsage)
        .arg(
            Arg::with_name("connect-timeout")
                .short("c")
                .takes_value(true)
                .long("connect-timeout")
                .help("Zookeeper connect timeout in milliseconds"),
        )
        .arg(
            Arg::with_name("hosts")
                .help("the zookeeper quorum string. ex: localhost:2181,localhost:2182"),
        )
        .get_matches();

    let hosts = matches.value_of("hosts");
    let timeout = value_t!(matches.value_of("connect-timeout"), u64).unwrap_or(4000);

    let shell = Shell::new(Duration::from_millis(timeout));
    if let Some(hosts) = hosts {
        let _ = shell.borrow_mut().init_connect(&hosts
            .split(",")
            .map(|x| x.to_string())
            .collect::<Vec<String>>());
    }

    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .build();

    let mut rl = Editor::with_config(config);

    let completer = Shell::get_completer(&shell);
    rl.set_completer(Some(completer));

    let history_file = match home_dir() {
        Some(path) => path.to_str().map(|p| format!("{}/.zkrsh_history", p)),
        None => None,
    };

    match history_file {
        Some(ref file) => {
            let _ = rl.load_history(file);
        }
        _ => {}
    };

    loop {
        let readline = rl.readline(shell.borrow().prompt().as_str());

        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_ref());
                if !shell.borrow_mut().process(&line) {
                    break;
                }
            }
            Err(ReadlineError::Interrupted) => break,
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    match history_file {
        Some(ref file) => {
            let _ = rl.save_history(file);
        }
        _ => {}
    };
}
