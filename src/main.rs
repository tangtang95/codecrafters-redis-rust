use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream}, thread, collections::HashMap, sync::{Arc, Mutex}, time::{SystemTime, Duration}, env, num::ParseIntError,
};
use anyhow::{anyhow, Context};

use crate::{tokenizer::{Resp, tokenize_bytes}, commands::{RedisCommands, InfoSection}};

mod tokenizer;
mod commands;

const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

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
    server_type: ServerType,
}

enum ServerType {
    Master(MasterStatus),
    Replica(ReplicaStatus)
}

struct MasterStatus {
    repl_id: String,
    repl_offset: u64,
    repl_tcp_streams: Vec<TcpStream>
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
    let _ = args.next();
    while let Some(arg) = args.next() {
        if arg.eq("--port") {
            let port_text = args.next().ok_or(anyhow!("port arg not found"))?;
            server_opts.port = port_text.parse::<u16>().with_context(|| "port is not a number between 0 and 65536")?;
        } else if arg.eq("--replicaof") {
            let master_host = args.next().ok_or(anyhow!("replicaof master host not found"))?;
            let master_port = args.next().ok_or(anyhow!("replicaof master pord not found"))?;
            let master_port = master_port.parse::<u16>().with_context(|| "master port is not a number between 0 and 65536")?;
            server_opts.replicaof = Some((master_host, master_port));
        } else {
            return Err(anyhow!("invalid cli arg \"{arg}\""))
        }
    }
    let listener = TcpListener::bind(format!("127.0.0.1:{}", server_opts.port))?;
    println!("Redis listening on port {}", server_opts.port);

    let redis_map = Arc::new(Mutex::new(HashMap::<String, Value>::new()));
    let server_type = match server_opts.replicaof {
        Some((master_address, master_port)) => ServerType::Replica(ReplicaStatus { master_address, master_port }),
        None => ServerType::Master(MasterStatus { 
            repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            repl_offset: 0,
            repl_tcp_streams: Vec::new() 
        })
    };

    if let ServerType::Replica(replica_status) = &server_type {
        let replica_info = ReplicaStatus { master_address: replica_status.master_address.clone(), master_port: replica_status.master_port };
        let redis_map = redis_map.clone();
        thread::spawn(move || {
            match connect_master(replica_info, server_opts.port, redis_map) {
                Ok(_) => println!("connection with master handled correctly"),
                Err(err) => println!("{}", err),
            }
        });
    }

    let server_opts = Arc::new(Mutex::new(ServerStatus {
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

fn connect_master(replica_info: ReplicaStatus, port: u16, redis_map: Arc<Mutex<HashMap<String, Value>>>) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(format!("{}:{}", replica_info.master_address, replica_info.master_port))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let ping_message = Resp::Array(vec![Resp::BulkString("ping".to_string())]);
    stream.write_all(ping_message.encode_to_string().as_bytes())?;
    println!("replica sent ping message");

    let mut bytes = [0u8; 512];
    let _ = stream.read(&mut bytes)?;
    let (_, tokens) = tokenize_bytes(&bytes)?;
    println!("replica handshake received: {:?}", tokens);
    if !tokens.eq(&Resp::SimpleString("PONG".to_string())) {
        return Err(anyhow!("wrong response from master"))
    }

    let replconf = Resp::Array(vec![
        Resp::BulkString("REPLCONF".to_string()),
        Resp::BulkString("listening-port".to_string()),
        Resp::BulkString(format!("{}", port))
    ]);
    stream.write_all(replconf.encode_to_string().as_bytes())?;
    println!("replica sent first replconf message");

    let mut bytes = [0u8; 512];
    let _ = stream.read(&mut bytes)?;
    let (_, tokens) = tokenize_bytes(&bytes)?;
    println!("replica handshake received: {:?}", tokens);
    if !tokens.eq(&Resp::SimpleString("OK".to_string())) {
        return Err(anyhow!("wrong response from master"))
    }

    let replconf = Resp::Array(vec![
        Resp::BulkString("REPLCONF".to_string()),
        Resp::BulkString("capa".to_string()),
        Resp::BulkString("psync2".to_string()),
    ]);
    stream.write_all(replconf.encode_to_string().as_bytes())?;
    println!("replica sent second replconf message");

    let mut bytes = [0u8; 512];
    let _ = stream.read(&mut bytes)?;
    let (_, tokens) = tokenize_bytes(&bytes)?;
    println!("replica handshake received: {:?}", tokens);
    if !tokens.eq(&Resp::SimpleString("OK".to_string())) {
        return Err(anyhow!("wrong response from master"))
    }

    let psync = Resp::Array(vec![
        Resp::BulkString("PSYNC".to_string()),
        Resp::BulkString("?".to_string()),
        Resp::BulkString("-1".to_string()),
    ]);
    stream.write_all(psync.encode_to_string().as_bytes())?;
    println!("replica sent psync message");

    let mut bytes = [0u8; 512];
    let _ = stream.read(&mut bytes)?;
    let (_, tokens) = tokenize_bytes(&bytes)?;
    println!("replica handshake received: {:?}", tokens);

    stream.set_read_timeout(None)?;
    loop {
        let mut bytes = [0u8; 512];
        let bytes_read = stream.read(&mut bytes)?;
        if bytes_read == 0 {
            return Ok(());
        }

        let (_, tokens) = tokenize_bytes(&bytes)?;
        println!("replica received: {:?}", tokens);
        let command: RedisCommands = tokens.try_into()?;
        if let RedisCommands::Set(opts) = command {
            redis_map.lock().unwrap()
                .insert(opts.key.to_string(), Value { value: opts.value.to_string(), expire: opts.expire, timestamp: SystemTime::now() });
        }
    }
}

fn handle_client(mut stream: TcpStream, redis_map: Arc<Mutex<HashMap<String, Value>>>, server_opts: Arc<Mutex<ServerStatus>>) -> anyhow::Result<()> {
    loop {
        let mut bytes = [0u8; 512];
        let bytes_read = stream.read(&mut bytes)?;
        if bytes_read == 0 {
            return Ok(());
        }

        let (_, tokens) = tokenize_bytes(&bytes)?;
        println!("received: {:?}", tokens);
        let command: RedisCommands = tokens.try_into()?;
        handle_command(&command, &mut stream, &redis_map, &server_opts)?;
        if let RedisCommands::PSync(_, _) = command {
            if let ServerType::Master(ref mut master_status) = server_opts.lock().unwrap().server_type {
                master_status.repl_tcp_streams.push(stream);
                return Ok(());
            }
        }
    }
}

fn handle_command(command: &RedisCommands, stream: &mut TcpStream, redis_map: &Arc<Mutex<HashMap<String, Value>>>, server_info: &Arc<Mutex<ServerStatus>>) -> anyhow::Result<()> {
    let response = match command {
        RedisCommands::Echo(text) => Resp::SimpleString(text.to_string()),
        RedisCommands::Ping => Resp::SimpleString("PONG".to_string()),
        RedisCommands::Set(options) => {
            redis_map.lock().unwrap()
                .insert(options.key.to_string(), Value { value: options.value.to_string(), expire: options.expire, timestamp: SystemTime::now() });
            if let ServerType::Master(ref mut master_status) = server_info.lock().unwrap().server_type {
                for repl_stream in &mut master_status.repl_tcp_streams {
                    let set_command = Resp::Array(vec![
                        Resp::BulkString("SET".to_string()),
                        Resp::BulkString(options.key.to_string()),
                        Resp::BulkString(options.value.to_string()),
                    ]);
                    repl_stream.write_all(&set_command.encode_to_bytes())?;
                }
            }
            Resp::SimpleString("OK".to_string())
        },
        RedisCommands::Get(key) => {
            let value = redis_map.lock().unwrap()
                .get(key)
                .filter(|k| {
                    if let Some(expire) = k.expire {
                        if let Ok(duration) = SystemTime::now().duration_since(k.timestamp) {
                            return duration < Duration::from_millis(expire);
                        }
                    }
                    true
                })
                .map(|k| k.value.to_string());
            if let Some(value) = value { Resp::BulkString(value) } else { Resp::NullBulkString }
        },
        RedisCommands::Info(info_section) => {
            match info_section {
                Some(InfoSection::Replication) => {
                    let info = server_info.lock().unwrap().server_type.encode_to_info_string();
                    Resp::BulkString(info)
                },
                None => {
                    let info = server_info.lock().unwrap().server_type.encode_to_info_string();
                    Resp::BulkString(info)
                },
            }
        },
        RedisCommands::ReplConf(_) => {
            Resp::SimpleString("OK".to_string())
        },
        RedisCommands::PSync(repl_id, repl_offset) => {
            match (repl_id.as_ref(), repl_offset) {
                ("?", -1) => {
                    let (master_repl_id, master_repl_offset) = match &server_info.lock().unwrap().server_type {
                        ServerType::Master(master_status) => (master_status.repl_id.clone(), master_status.repl_offset),
                        ServerType::Replica(_) => unimplemented!(),
                    };
                    let response = Resp::SimpleString(format!("FULLRESYNC {} {}", master_repl_id, master_repl_offset));
                    let empty_rdb_bytes = decode_hex(EMPTY_RDB)?;
                    let empty_rdb_bytes = [b"$", empty_rdb_bytes.len().to_string().as_bytes(), b"\r\n", &empty_rdb_bytes].concat();
                    stream.write_all(&[&response.encode_to_bytes(), empty_rdb_bytes.as_slice()].concat())?;
                    Resp::Empty
                },
                _ => unimplemented!()
            }
        }
    };
    stream.write_all(response.encode_to_string().as_bytes())?;
    Ok(())
}

fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}
