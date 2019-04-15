use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum Command {
    Cd,
    Connect,
    Create,
    Disconnect,
    Exit,
    Get,
    Help,
    Ls,
    Rm,
    Set,
    Stat,
    Pwd,
    // TODO
    // Copy,
    // Watch,
    // Rmr,
}

lazy_static! {
    pub static ref COMMAND_MAP: HashMap<&'static str, Command> = {
        let mut m = HashMap::new();
        m.insert("cd", Command::Cd);
        m.insert("create", Command::Create);
        m.insert("ls", Command::Ls);
        m.insert("get", Command::Get);
        m.insert("rm", Command::Rm);
        m.insert("set", Command::Set);
        m.insert("stat", Command::Stat);
        m.insert("pwd", Command::Pwd);
        m.insert("connect", Command::Connect);
        m.insert("disconnect", Command::Disconnect);
        m.insert("exit", Command::Exit);
        m.insert("help", Command::Help);
        m
    };
}
