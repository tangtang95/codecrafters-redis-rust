use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream}, thread, collections::HashMap, sync::{Arc, Mutex}, time::{SystemTime, Duration}, env,
};

use anyhow::{anyhow, Context};

struct Value {
    value: String,
    expire: Option<u64>,
    timestamp: SystemTime
}

struct ServerOptions {
    port: u16,
    replicaof: Option<(String, u16)>
}

struct ServerInfo {
    master_info: Option<MasterReplInfo>,
    slave_info: Option<(String, u16)>
}

struct MasterReplInfo {
    repl_id: String,
    repl_offset: u64
}

fn main() -> anyhow::Result<()>{
    let mut args = env::args();
    let mut server_opts = ServerOptions { port: 6379, replicaof: None };
    while let Some(arg) = args.next() {
        if arg.eq("--port") {
            let port_text = args.next().ok_or(anyhow!("port arg not found"))?;
            server_opts.port = port_text.parse::<u16>().with_context(|| "port is not a number between 0 and 65536")?;
        }
        if arg.eq("--replicaof") {
            let master_host = args.next().ok_or(anyhow!("replicaof master host not found"))?;
            let master_port = args.next().ok_or(anyhow!("replicaof master pord not found"))?;
            let master_port = master_port.parse::<u16>().with_context(|| "master port is not a number between 0 and 65536")?;
            server_opts.replicaof = Some((master_host, master_port));
        }
    }
    let listener = TcpListener::bind(format!("127.0.0.1:{}", server_opts.port))?;
    println!("Redis listening on port {}", server_opts.port);

    let redis_map = Arc::new(Mutex::new(HashMap::<String, Value>::new()));
    let master_info = match server_opts.replicaof {
        Some(_) => None,
        None => Some(MasterReplInfo { repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(), repl_offset: 0})
    };
    let server_opts = Arc::new(Mutex::new(ServerInfo {
        master_info,
        slave_info: server_opts.replicaof
    }));

    let mut socket_id: u64 = 0;
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let _socket_id = socket_id;
                let redis_map = redis_map.clone();
                let server_opts = server_opts.clone();

                println!("accepted new connection socket {}", _socket_id);
                thread::spawn(move || {
                    match handle_client(_stream, redis_map, server_opts) {
                        Ok(_) => println!("connection {} handled correctly", _socket_id),
                        Err(err) => println!("{}", err),
                    }
                });
                socket_id += 1;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, redis_map: Arc<Mutex<HashMap<String, Value>>>, server_opts: Arc<Mutex<ServerInfo>>) -> anyhow::Result<()> {
    loop {
        let mut bytes = [0u8; 512];
        let bytes_read = stream.read(&mut bytes)?;
        if bytes_read == 0 {
            return Ok(());
        }

        let buf = String::from_utf8(bytes.to_vec())?.trim_end_matches('\0').to_string();
        println!("received: {}", buf);
        let (_, tokens) = tokenize(&buf)?;
        let command: RedisCommands = tokens.try_into()?;
        handle_command(command, &mut stream, redis_map.clone(), server_opts.clone())?;
    }
}

#[derive(PartialEq)]
enum Resp<'a> {
    Array(Vec<Resp<'a>>),
    BulkString(String),
    SimpleString(&'a str),
    Integer(i64),
    NullBulkString
}

impl<'a> Resp<'a> {
    pub fn encode_to_string(&self) -> String {
        match self {
            Resp::Array(_) => todo!(),
            Resp::BulkString(string) => format!("${}\r\n{}\r\n", string.len(), string),
            Resp::SimpleString(string) => format!("+{}\r\n", string),
            Resp::NullBulkString => "$-1\r\n".to_string(),
            Resp::Integer(num) => format!(":{}\r\n", num),
        }
    }
}

enum RedisCommands {
    Echo(String),
    Ping,
    Set(SetOptions),
    Get(String),
    Info(Option<InfoSection>),
}

struct SetOptions {
    key: String,
    value: String,
    expire: Option<u64>
}

enum InfoSection {
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

impl<'a> TryFrom<Resp<'a>> for RedisCommands {
    type Error = anyhow::Error;

    fn try_from(value: Resp<'a>) -> Result<Self, Self::Error> {
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

fn tokenize(resp: &str) -> anyhow::Result<(&str, Resp)> {
    let resp_type = resp.get(0..1).ok_or(anyhow!("RESP type not found"))?;
    match resp_type {
       "*" => {
            let line = resp.lines().next().ok_or(anyhow!("RESP next line not found"))?;
            let len = line.trim_start_matches('*').parse::<usize>()?;
            let mut vec: Vec<Resp> = Vec::new();
            let mut remainder = resp.get(line.len()+2..).ok_or(anyhow!("RESP out of bounds"))?;
            for _ in 0..len {
                let (new_remainder, child_resp) = tokenize(remainder)?;
                vec.push(child_resp);
                remainder = new_remainder;
            }
            Ok((remainder, Resp::Array(vec)))
        },
       "$" => {
            let mut consumed_len = 0;
            let mut line_iter = resp.lines();
            let line = line_iter.next().ok_or(anyhow!("RESP next line not found"))?;
            consumed_len += line.len() + 2;
            let len = line.trim_start_matches('$').parse::<usize>()?;

            let line = line_iter.next().ok_or(anyhow!("RESP next line not found"))?;
            if len != line.trim_end().len() {
                return Err(anyhow!("RESP bulk string len does not coincide"));
            }
            consumed_len += line.len() + 2;

            let remainder = resp.get(consumed_len..).ok_or(anyhow!("RESP out of bounds"))?;
            Ok((remainder, Resp::BulkString(line.to_owned())))
        },
        ":" => {
            let line = resp.lines().next().ok_or(anyhow!("RESP next line not found"))?;
            let integer = line.trim_start_matches(':').parse::<i64>()?;
            let remainder = resp.get(line.len()+2..).ok_or(anyhow!("RESP out of bounds"))?;
            Ok((remainder, Resp::Integer(integer)))
        },
        _ => {
            println!("RESP type `{:?}` not implemented", resp_type);
            unimplemented!()
        }
    }
}

fn handle_command(command: RedisCommands, stream: &mut TcpStream, redis_map: Arc<Mutex<HashMap<String, Value>>>, server_info: Arc<Mutex<ServerInfo>>) -> anyhow::Result<()> {
    match command {
        RedisCommands::Echo(text) => {
            let response = Resp::SimpleString(&text);
            stream.write_all(response.encode_to_string().as_bytes())?;
            Ok(())
        },
        RedisCommands::Ping => {
            let pong = Resp::SimpleString("PONG");
            stream.write_all(pong.encode_to_string().as_bytes())?;
            Ok(())
        },
        RedisCommands::Set(options) => {
            redis_map.lock().unwrap()
                .insert(options.key, Value { value: options.value, expire: options.expire, timestamp: SystemTime::now() });
            let ok = Resp::SimpleString("OK");
            stream.write_all(ok.encode_to_string().as_bytes())?;
            Ok(())
        },
        RedisCommands::Get(key) => {
            let value = redis_map.lock().unwrap()
                .get(&key)
                .filter(|k| {
                    if let Some(expire) = k.expire {
                        if let Ok(duration) = SystemTime::now().duration_since(k.timestamp) {
                            return duration < Duration::from_millis(expire);
                        }
                    }
                    true
                })
                .map(|k| k.value.to_string());
            if let Some(value) = value {
                let value = Resp::BulkString(value);
                stream.write_all(value.encode_to_string().as_bytes())?;
            } else {
                let null = Resp::NullBulkString;
                stream.write_all(null.encode_to_string().as_bytes())?;
            }
            Ok(())
        },
        RedisCommands::Info(info_section) => {
            match info_section {
                Some(InfoSection::Replication) => {
                    let response = {
                        let server_info = server_info.lock().unwrap();
                        let role = match server_info.slave_info {
                            Some(_) => "slave",
                            None => "master",
                        };
                        let master_response = match &server_info.master_info {
                            Some(master_info) => format!("\r\nmaster_replid:{}\r\nmaster_repl_offset:{}", master_info.repl_id, master_info.repl_offset),
                            None => String::new()
                        };
                        format!("role:{role}") + &master_response
                    };
                    let repl_info = Resp::BulkString(response);
                    stream.write_all(repl_info.encode_to_string().as_bytes())?;
                },
                None => {
                    let response = {
                        let server_info = server_info.lock().unwrap();
                        let role = match server_info.slave_info {
                            Some(_) => "slave",
                            None => "master",
                        };
                        let master_response = match &server_info.master_info {
                            Some(master_info) => format!("\r\nmaster_replid:{}\r\nmaster_repl_offset:{}", master_info.repl_id, master_info.repl_offset),
                            None => String::new()
                        };
                        format!("role:{role}") + &master_response
                    };
                    let repl_info = Resp::BulkString(response);
                    stream.write_all(repl_info.encode_to_string().as_bytes())?;
                },
            };
            Ok(())
        },
    }
}
