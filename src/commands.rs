use anyhow::anyhow;

use crate::tokenizer::Resp;

pub enum RedisCommands {
    Echo(String),
    Ping,
    Set(SetOptions),
    Get(String),
    Info(Option<InfoSection>),
    ReplConf(ReplConfMode)
}

pub struct SetOptions {
    pub key: String,
    pub value: String,
    pub expire: Option<u64>
}

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

pub enum ReplConfMode {
    ListeningPort(u16),
    Capability(String)
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
            mode => Err(anyhow!("info section {mode} not supported"))
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
            _ => unimplemented!()
        }
    }
}
