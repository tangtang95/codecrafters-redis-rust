use anyhow::anyhow;

use crate::tokenizer::Resp;

pub enum RedisCommands {
    Echo(String),
    Ping,
    Set(SetOptions),
    Get(String),
    Info(Option<InfoSection>),
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
            _ => unimplemented!()
        }
    }
}
