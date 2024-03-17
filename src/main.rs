use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream}, thread, collections::HashMap, sync::{Arc, Mutex}, time::{SystemTime, Duration}, env,
};
use anyhow::{anyhow, Context};

use crate::{tokenizer::{tokenize, Resp}, commands::{RedisCommands, InfoSection}};

mod tokenizer;
mod commands;

struct Value {
    value: String,
    expire: Option<u64>,
    timestamp: SystemTime
}

struct ServerOptions {
    port: u16,
    replicaof: Option<(String, u16)>
}

struct ServerStatus {
    port: u16,
    server_type: ServerType,
}

enum ServerType {
    Master(MasterStatus),
    Replica(ReplicaStatus)
}

struct MasterStatus {
    repl_id: String,
    repl_offset: u64
}

struct ReplicaStatus {
    master_address: String,
    master_port: u16
}

impl ServerType {
    fn encode_to_info_string(&self) -> String {
        match self {
            ServerType::Master(status) => 
                format!("role:master\r\n\
                    master_replid:{}\r\n\
                    master_repl_offset:{}", 
                    status.repl_id, status.repl_offset
                ),
            ServerType::Replica(_) => 
                "role:slave".to_string(),
        }
    }
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
    let server_type = match server_opts.replicaof {
        Some((master_address, master_port)) => ServerType::Replica(ReplicaStatus { master_address, master_port }),
        None => ServerType::Master(MasterStatus { repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(), repl_offset: 0})
    };
    let server_opts = Arc::new(Mutex::new(ServerStatus {
        port: server_opts.port,
        server_type
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

fn handle_client(mut stream: TcpStream, redis_map: Arc<Mutex<HashMap<String, Value>>>, server_opts: Arc<Mutex<ServerStatus>>) -> anyhow::Result<()> {
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

fn handle_command(command: RedisCommands, stream: &mut TcpStream, redis_map: Arc<Mutex<HashMap<String, Value>>>, server_info: Arc<Mutex<ServerStatus>>) -> anyhow::Result<()> {
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
                    let info = server_info.lock().unwrap().server_type.encode_to_info_string();
                    let repl_info = Resp::BulkString(info);
                    stream.write_all(repl_info.encode_to_string().as_bytes())?;
                },
                None => {
                    let info = server_info.lock().unwrap().server_type.encode_to_info_string();
                    let repl_info = Resp::BulkString(info);
                    stream.write_all(repl_info.encode_to_string().as_bytes())?;
                },
            };
            Ok(())
        },
    }
}
