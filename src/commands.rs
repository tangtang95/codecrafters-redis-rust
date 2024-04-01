use anyhow::anyhow;

use crate::tokenizer::Resp;

#[derive(Debug, Clone)]
pub enum RedisCommands {
    Echo(String),
    Ping,
    Set(SetOptions),
    Get(String),
    Info(Option<InfoSection>),
    ReplConf(ReplConfMode),
    PSync(String, i64),
    Wait(i32, u64)
}

#[derive(Debug, Clone)]
pub struct SetOptions {
    pub key: String,
    pub value: String,
    pub expire: Option<u64>
}


#[derive(Debug, Clone)]
pub enum InfoSection {
    Replication
}

impl TryFrom<&str> for InfoSection {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_ref() {
            "replication" => Ok(InfoSection::Replication),
            section => Err(anyhow!("info section {section} not supported"))
        }
    }
}

impl From<InfoSection> for Resp {
    fn from(val: InfoSection) -> Self {
        match val {
            InfoSection::Replication => Resp::BulkString("REPLICATION".to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReplConfMode {
    ListeningPort(u16),
    Capability(String),
    GetAck(String),
    Ack(i64)
}

impl TryFrom<(&str, &str)> for ReplConfMode {
    type Error = anyhow::Error;

    fn try_from(value: (&str, &str)) -> Result<Self, Self::Error> {
        match value.0.to_lowercase().as_ref() {
            "listening-port" => {
                let port = value.1.parse::<u16>()?;
                Ok(ReplConfMode::ListeningPort(port))
            },
            "capa" => Ok(ReplConfMode::Capability(value.1.to_string())),
            "getack" => Ok(ReplConfMode::GetAck(value.1.to_string())),
            "ack" => Ok(ReplConfMode::Ack(value.1.parse::<i64>()?)),
            mode => Err(anyhow!("info section {mode} not supported"))
        }
    }
}

impl From<ReplConfMode> for Vec<Resp> {
    fn from(val: ReplConfMode) -> Self {
        match val {
            ReplConfMode::ListeningPort(port) => vec![
                Resp::BulkString("LISTENING-PORT".to_string()),
                Resp::BulkString(port.to_string())
            ],
            ReplConfMode::Capability(capa) => vec![
                Resp::BulkString("CAPA".to_string()),
                Resp::BulkString(capa)
            ],
            ReplConfMode::GetAck(ack) => vec![
                Resp::BulkString("GETACK".to_string()),
                Resp::BulkString(ack)
            ],
            ReplConfMode::Ack(offset) => vec![
                Resp::BulkString("ACK".to_string()),
                Resp::BulkString(offset.to_string())
            ],
        }
    }
}

impl TryFrom<Resp> for RedisCommands {
    type Error = anyhow::Error;

    fn try_from(value: Resp) -> Result<Self, Self::Error> {
        let Resp::Array(array) = value else { return Err(anyhow!("Command failed"))};
        let Some(Resp::BulkString(command)) = array.first() else { return Err(anyhow!("Command failed"))};
        match command.to_lowercase().as_ref() {
            "ping" => Ok(RedisCommands::Ping),
            "echo" => {
                match array.get(1) {
                    Some(Resp::BulkString(text)) => Ok(RedisCommands::Echo(text.to_string())),
                    _ => Err(anyhow!("Echo arg not supported"))
                }
            },
            "set" => { 
                match array.get(1..3) {
                    Some([Resp::BulkString(key), Resp::BulkString(value)]) => {
                        let expire = match array.get(3..5) {
                            Some([Resp::BulkString(option), Resp::BulkString(value)]) => {
                                if option.eq_ignore_ascii_case("px") {
                                    let value = value.parse::<u64>()?;
                                    Some(value)
                                } else {
                                    None
                                }
                            },
                            _ => None
                        };
                        Ok(RedisCommands::Set(SetOptions {
                            key: key.to_string(),
                            value: value.to_string(),
                            expire
                        }))
                    },
                    _ => Err(anyhow!("Set arg not supported"))
                }
            },
            "get" => { 
                match array.get(1) {
                    Some(Resp::BulkString(text)) => Ok(RedisCommands::Get(text.to_string())),
                    _ => Err(anyhow!("Get arg not supported"))
                }
            },
            "info" => {
                match array.get(1) {
                    Some(Resp::BulkString(section)) => Ok(RedisCommands::Info(Some(section.as_str().try_into()?))),
                    None => Ok(RedisCommands::Info(None)),
                    _ => Err(anyhow!("Info arg not supported"))
                }
            },
            "replconf" => {
                let Some(Resp::BulkString(mode)) = array.get(1) else { return Err(anyhow!("ReplConf mode missing")) };
                let Some(Resp::BulkString(mode_arg)) = array.get(2) else { return Err(anyhow!("ReplConf second arg missing")) };
                let mode = ReplConfMode::try_from((mode.as_ref(), mode_arg.as_ref()))?;
                Ok(RedisCommands::ReplConf(mode))
            },
            "psync" => {
                let Some(Resp::BulkString(repl_id)) = array.get(1) else { return Err(anyhow!("PSync repl_id missing")) };
                let Some(Resp::BulkString(repl_offset)) = array.get(2) else { return Err(anyhow!("PSync repl_offset missing")) };
                let repl_offset = repl_offset.parse::<i64>()?;
                Ok(RedisCommands::PSync(repl_id.to_string(), repl_offset))
            },
            _ => unimplemented!()
        }
    }
}

impl From<RedisCommands> for Resp {
    fn from(val: RedisCommands) -> Self {
        match val {
            RedisCommands::Echo(text) => Resp::Array(vec![
                Resp::BulkString("ECHO".to_string()),
                Resp::BulkString(text)
            ]),
            RedisCommands::Ping => Resp::Array(vec![
                Resp::BulkString("PING".to_string()),
            ]),
            RedisCommands::Set(opts) => {
                let mut set_cmd = vec![
                    Resp::BulkString("SET".to_string()),
                    Resp::BulkString(opts.key),
                    Resp::BulkString(opts.value),
                ];
                if let Some(expire) = opts.expire {
                    set_cmd.push(Resp::BulkString("PX".to_string()));
                    set_cmd.push(Resp::BulkString(expire.to_string()));
                }
                Resp::Array(set_cmd)
            },
            RedisCommands::Get(key) => Resp::Array(vec![
                Resp::BulkString("GET".to_string()),
                Resp::BulkString(key),
            ]),
            RedisCommands::Info(section) => {
                let mut info_cmd = vec![
                    Resp::BulkString("INFO".to_string()),
                ];
                if let Some(section) = section {
                    info_cmd.push(section.into());
                }
                Resp::Array(info_cmd)
            },
            RedisCommands::ReplConf(mode) => {
                let mut replconf_cmd = vec![
                    Resp::BulkString("REPLCONF".to_string()), 
                ];
                let mode_resp: Vec<Resp> = mode.into();
                replconf_cmd.extend(mode_resp);
                Resp::Array(replconf_cmd)
            },
            RedisCommands::PSync(repl_id, repl_offset) => Resp::Array(vec![
                Resp::BulkString("PSYNC".to_string()),
                Resp::BulkString(repl_id),
                Resp::BulkString(repl_offset.to_string()),
            ]),
            RedisCommands::Wait(num_replicas, timeout) => Resp::Array(vec![
                Resp::BulkString("WAIT".to_string()),
                Resp::BulkString(num_replicas.to_string()),
                Resp::BulkString(timeout.to_string()),
            ])
        }
    }
}
