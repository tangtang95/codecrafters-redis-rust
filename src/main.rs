use anyhow::{anyhow, Context};
use std::{
    collections::HashMap,
    env,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    num::ParseIntError,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime}, path::PathBuf, str::FromStr,
};

use crate::{
    commands::{InfoSection, RedisCommands},
    tokenizer::{read_next_line, tokenize_bytes, Resp},
};

mod commands;
mod tokenizer;

const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

struct Value {
    value: String,
    expire: Option<u64>,
    timestamp: SystemTime,
}

struct ServerOptions {
    port: u16,
    replicaof: Option<(String, u16)>,
    dir: Option<PathBuf>,
    db_filename: Option<String>,
}

struct ServerStatus {
    server_type: ServerType,
}

enum ServerType {
    Master(MasterStatus),
    Replica(ReplicaStatus),
}

struct MasterStatus {
    repl_id: String,
    repl_offset: u64,
    repl_data_offset: u64,
    replicas_data: Vec<ReplicaData>,
    dir: Option<PathBuf>,
    db_filename: Option<String>
}

struct ReplicaData {
    stream: TcpStream,
    latest_offset: u64,
}

struct ReplicaStatus {
    master_address: String,
    master_port: u16,
}

impl ServerType {
    fn encode_to_info_string(&self) -> String {
        match self {
            ServerType::Master(status) => format!(
                "role:master\r\n\
                    master_replid:{}\r\n\
                    master_repl_offset:{}",
                status.repl_id, status.repl_offset
            ),
            ServerType::Replica(_) => "role:slave".to_string(),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut args = env::args();
    let mut server_opts = ServerOptions {
        port: 6379,
        replicaof: None,
        dir: None,
        db_filename: None
    };
    let _ = args.next();
    while let Some(arg) = args.next() {
        if arg.eq("--port") {
            let port_text = args.next().ok_or(anyhow!("port arg not found"))?;
            server_opts.port = port_text
                .parse::<u16>()
                .with_context(|| "port is not a number between 0 and 65536")?;
        } else if arg.eq("--replicaof") {
            let master_host = args.next().ok_or(anyhow!("replicaof master host not found"))?;
            let master_port = args.next().ok_or(anyhow!("replicaof master pord not found"))?;
            let master_port = master_port
                .parse::<u16>()
                .with_context(|| "master port is not a number between 0 and 65536")?;
            server_opts.replicaof = Some((master_host, master_port));
        } else if arg.eq("--dir") {
            let dir = args.next().ok_or(anyhow!("dir path arg not found"))?;
            server_opts.dir = Some(PathBuf::from_str(&dir)?);
        } else if arg.eq("--dbfilename") {
            let db_filename = args.next().ok_or(anyhow!("dbfilename arg not found"))?;
            server_opts.db_filename = Some(db_filename);
        } else {
            return Err(anyhow!("invalid cli arg \"{arg}\""));
        }
    }
    let listener = TcpListener::bind(format!("127.0.0.1:{}", server_opts.port))?;
    println!("Redis listening on port {}", server_opts.port);

    let redis_map = Arc::new(Mutex::new(HashMap::<String, Value>::new()));
    let server_type = match server_opts.replicaof {
        Some((master_address, master_port)) => ServerType::Replica(ReplicaStatus {
            master_address,
            master_port,
        }),
        None => ServerType::Master(MasterStatus {
            repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            repl_offset: 0,
            repl_data_offset: 0,
            replicas_data: Vec::new(),
            dir: server_opts.dir,
            db_filename: server_opts.db_filename
        }),
    };

    if let ServerType::Replica(replica_status) = &server_type {
        let replica_info = ReplicaStatus {
            master_address: replica_status.master_address.clone(),
            master_port: replica_status.master_port,
        };
        let redis_map = redis_map.clone();
        thread::spawn(
            move || match connect_master(replica_info, server_opts.port, redis_map) {
                Ok(_) => println!("connection with master handled correctly"),
                Err(err) => println!("{}", err),
            },
        );
    }

    let server_opts = Arc::new(Mutex::new(ServerStatus { server_type }));

    let mut socket_id: u64 = 0;
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let _socket_id = socket_id;
                let redis_map = redis_map.clone();
                let server_opts = server_opts.clone();

                println!("accepted new connection socket {}", _socket_id);
                thread::spawn(move || match handle_client(_stream, redis_map, server_opts) {
                    Ok(_) => println!("connection {} handled correctly", _socket_id),
                    Err(err) => println!("{}", err),
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

fn connect_master(
    replica_info: ReplicaStatus,
    port: u16,
    redis_map: Arc<Mutex<HashMap<String, Value>>>,
) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(format!("{}:{}", replica_info.master_address, replica_info.master_port))?;
    let mut buf_reader = BufReader::new(stream.try_clone()?);

    let ping_message = Resp::Array(vec![Resp::BulkString("ping".to_string())]);
    stream.write_all(ping_message.encode_to_string().as_bytes())?;
    println!("replica sent ping message");

    let bytes = buf_reader.fill_buf()?;
    let (remainder, tokens) = tokenize_bytes(bytes)?;
    let consumed_bytes = bytes.len() - remainder.len();
    buf_reader.consume(consumed_bytes);
    println!("replica handshake received: {:?}", tokens);
    if !tokens.eq(&Resp::SimpleString("PONG".to_string())) {
        return Err(anyhow!("wrong response from master"));
    }

    let replconf = Resp::Array(vec![
        Resp::BulkString("REPLCONF".to_string()),
        Resp::BulkString("listening-port".to_string()),
        Resp::BulkString(format!("{}", port)),
    ]);
    stream.write_all(replconf.encode_to_string().as_bytes())?;
    println!("replica sent first replconf message");

    let bytes = buf_reader.fill_buf()?;
    let (remainder, tokens) = tokenize_bytes(bytes)?;
    let consumed_bytes = bytes.len() - remainder.len();
    buf_reader.consume(consumed_bytes);
    println!("replica handshake received: {:?}", tokens);
    if !tokens.eq(&Resp::SimpleString("OK".to_string())) {
        return Err(anyhow!("wrong response from master"));
    }

    let replconf = Resp::Array(vec![
        Resp::BulkString("REPLCONF".to_string()),
        Resp::BulkString("capa".to_string()),
        Resp::BulkString("psync2".to_string()),
    ]);
    stream.write_all(replconf.encode_to_string().as_bytes())?;
    println!("replica sent second replconf message");

    let bytes = buf_reader.fill_buf()?;
    let (remainder, tokens) = tokenize_bytes(bytes)?;
    let consumed_bytes = bytes.len() - remainder.len();
    buf_reader.consume(consumed_bytes);
    println!("replica handshake received: {:?}", tokens);
    if !tokens.eq(&Resp::SimpleString("OK".to_string())) {
        return Err(anyhow!("wrong response from master"));
    }

    let psync = Resp::Array(vec![
        Resp::BulkString("PSYNC".to_string()),
        Resp::BulkString("?".to_string()),
        Resp::BulkString("-1".to_string()),
    ]);
    stream.write_all(psync.encode_to_string().as_bytes())?;
    println!("replica sent psync message");

    let bytes = buf_reader.fill_buf()?;
    let (remainder, tokens) = tokenize_bytes(bytes)?;
    let consumed_bytes = bytes.len() - remainder.len();
    buf_reader.consume(consumed_bytes);
    println!("replica handshake received: {:?}", tokens);
    let mut ack_offset = match tokens {
        Resp::SimpleString(resync_text) if resync_text.starts_with("FULLRESYNC") => {
            let split_text: Vec<&str> = resync_text.split_ascii_whitespace().collect();
            split_text.get(2).unwrap_or(&"0").parse::<i64>()?
        }
        _ => return Err(anyhow!("wrong response from master")),
    };
    // Read RDB bytes
    let bytes = buf_reader.fill_buf()?;
    let (remainder, rdb_len_line) = read_next_line(bytes)?;
    let consumed_bytes = bytes.len() - remainder.len();
    let rdb_bytes_len = String::from_utf8(rdb_len_line[1..].to_vec())?.parse::<usize>()?;
    buf_reader.consume(consumed_bytes);
    buf_reader.consume(rdb_bytes_len);

    loop {
        let bytes = buf_reader.fill_buf()?;
        if bytes.is_empty() {
            return Ok(());
        }

        let remainder = match tokenize_bytes(bytes) {
            Ok((remainder, tokens)) => {
                println!("received from master: {:?}", tokens);
                let command: RedisCommands = tokens.try_into()?;
                handle_master_command(&command, &mut stream, &redis_map, ack_offset)?;
                remainder
            }
            Err(err) => {
                println!("skip buffer since untokenizable: {}", err);
                bytes
            }
        };
        let consumed_bytes = bytes.len() - remainder.len();
        ack_offset += consumed_bytes as i64;
        buf_reader.consume(consumed_bytes);
    }
}

fn handle_master_command(
    command: &RedisCommands,
    stream: &mut TcpStream,
    redis_map: &Arc<Mutex<HashMap<String, Value>>>,
    ack_offset: i64,
) -> anyhow::Result<()> {
    match command {
        RedisCommands::Ping => {
            println!("replica received ping from master");
        }
        RedisCommands::Set(opts) => {
            redis_map.lock().unwrap().insert(
                opts.key.to_string(),
                Value {
                    value: opts.value.to_string(),
                    expire: opts.expire,
                    timestamp: SystemTime::now(),
                },
            );
        }
        RedisCommands::ReplConf(commands::ReplConfMode::GetAck(_)) => {
            let response = RedisCommands::ReplConf(commands::ReplConfMode::Ack(ack_offset));
            stream.write_all(&Resp::from(response).encode_to_bytes())?;
        }
        _ => {
            println!("replica ignore command from master: {:?}", command);
        }
    };
    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    redis_map: Arc<Mutex<HashMap<String, Value>>>,
    server_opts: Arc<Mutex<ServerStatus>>,
) -> anyhow::Result<()> {
    let mut buf_reader = BufReader::new(stream.try_clone()?);
    loop {
        let bytes = buf_reader.fill_buf()?;
        if bytes.is_empty() {
            return Ok(());
        }

        let remainder = match tokenize_bytes(bytes) {
            Ok((remainder, tokens)) => {
                println!("received: {:?}", tokens);
                let command: RedisCommands = tokens.try_into()?;
                handle_command(&command, &mut stream, &redis_map, &server_opts)?;
                if let RedisCommands::PSync(_, _) = command {
                    if let ServerType::Master(ref mut master_status) = server_opts.lock().unwrap().server_type {
                        let stream_clone = stream.try_clone()?;
                        let server_state = server_opts.clone();
                        let index = master_status.replicas_data.len();
                        thread::spawn(move || {
                            handle_replica_commands(stream_clone, server_state, index).unwrap();
                        });
                        master_status.replicas_data.push(ReplicaData {
                            stream,
                            latest_offset: 0,
                        });
                        println!("master added a replica");
                        return Ok(());
                    }
                }
                remainder
            }
            Err(err) => {
                println!("skip buffer since untokenizable: {}", err);
                bytes
            }
        };
        let consumed_bytes = bytes.len() - remainder.len();
        buf_reader.consume(consumed_bytes);
    }
}

fn handle_command(
    command: &RedisCommands,
    stream: &mut impl Write,
    redis_map: &Arc<Mutex<HashMap<String, Value>>>,
    server_info: &Arc<Mutex<ServerStatus>>,
) -> anyhow::Result<()> {
    let response = match command {
        RedisCommands::Echo(text) => Resp::SimpleString(text.to_string()),
        RedisCommands::Ping => Resp::SimpleString("PONG".to_string()),
        RedisCommands::Set(options) => {
            redis_map.lock().unwrap().insert(
                options.key.to_string(),
                Value {
                    value: options.value.to_string(),
                    expire: options.expire,
                    timestamp: SystemTime::now(),
                },
            );
            match server_info.lock().unwrap().server_type {
                ServerType::Master(ref mut master_status) => {
                    let set_command = Resp::from(command.clone());
                    master_status.repl_offset += set_command.encode_to_bytes().len() as u64;
                    master_status.repl_data_offset = master_status.repl_offset;
                    for replica_data in &mut master_status.replicas_data {
                        replica_data.stream.write_all(&set_command.encode_to_bytes())?;
                    }
                }
                ServerType::Replica(_) => {
                    unimplemented!()
                }
            };

            Resp::SimpleString("OK".to_string())
        }
        RedisCommands::Get(key) => {
            let value = redis_map
                .lock()
                .unwrap()
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
            if let Some(value) = value {
                Resp::BulkString(value)
            } else {
                Resp::NullBulkString
            }
        }
        RedisCommands::Info(info_section) => match info_section {
            Some(InfoSection::Replication) => {
                let info = server_info.lock().unwrap().server_type.encode_to_info_string();
                Resp::BulkString(info)
            }
            None => {
                let info = server_info.lock().unwrap().server_type.encode_to_info_string();
                Resp::BulkString(info)
            }
        },
        RedisCommands::ReplConf(_) => Resp::SimpleString("OK".to_string()),
        RedisCommands::PSync(repl_id, repl_offset) => match (repl_id.as_ref(), repl_offset) {
            ("?", -1) => {
                let (master_repl_id, master_repl_offset) = match &server_info.lock().unwrap().server_type {
                    ServerType::Master(master_status) => (master_status.repl_id.clone(), master_status.repl_offset),
                    ServerType::Replica(_) => unimplemented!(),
                };
                let response = Resp::SimpleString(format!("FULLRESYNC {} {}", master_repl_id, master_repl_offset));
                let empty_rdb_bytes = decode_hex(EMPTY_RDB)?;
                let empty_rdb_bytes = [
                    b"$",
                    empty_rdb_bytes.len().to_string().as_bytes(),
                    b"\r\n",
                    &empty_rdb_bytes,
                ]
                .concat();
                stream.write_all(&[&response.encode_to_bytes(), empty_rdb_bytes.as_slice()].concat())?;
                Resp::Empty
            }
            _ => unimplemented!(),
        },
        RedisCommands::Wait(num_replicas, timeout) => {
            let start_time = SystemTime::now();
            let (mut replica_streams, master_data_offset) = match server_info.lock().unwrap().server_type {
                ServerType::Master(ref master_status) => {
                    let mut streams = vec![];
                    for replica_data in &master_status.replicas_data {
                        streams.push(replica_data.stream.try_clone()?)
                    }
                    (streams, master_status.repl_data_offset)
                }
                ServerType::Replica(_) => (vec![], 0),
            };

            if master_data_offset == 0 {
                Resp::Integer(replica_streams.len() as i64)
            } else {
                println!("[wait]: master_offset: {}", master_data_offset);
                let num_replicas = *num_replicas;

                let mut last_replica_oks = 0;
                for stream in replica_streams.as_mut_slice() {
                    let getack_command = RedisCommands::ReplConf(commands::ReplConfMode::GetAck("*".to_string()));
                    stream.write_all(&Resp::from(getack_command).encode_to_bytes())?;
                }
                if let ServerType::Master(master_status) = &mut server_info.lock().unwrap().server_type {
                    let getack_command = RedisCommands::ReplConf(commands::ReplConfMode::GetAck("*".to_string()));
                    let getack_resp = Resp::from(getack_command.clone());
                    master_status.repl_offset += getack_resp.encode_to_bytes().len() as u64;
                };
                let replica_oks = loop {
                    let replica_oks = if let ServerType::Master(state) = &server_info.lock().unwrap().server_type {
                        state
                            .replicas_data
                            .iter()
                            .map(|replica_data| (replica_data.latest_offset >= master_data_offset) as i32)
                            .sum()
                    } else {
                        0
                    };
                    if replica_oks >= num_replicas {
                        break replica_oks;
                    } else if Duration::from_millis(*timeout) <= SystemTime::now().duration_since(start_time)? {
                        break last_replica_oks;
                    }
                    thread::sleep(Duration::from_millis(20));
                    last_replica_oks = replica_oks;
                };
                Resp::Integer(replica_oks as i64)
            }
        },
        RedisCommands::Config(mode, config_key) => {
            if mode.eq_ignore_ascii_case("GET") {
                match config_key.as_str() {
                    "dir" => match &server_info.lock().unwrap().server_type {
                        ServerType::Master(state) => {
                            let dir = state.dir.as_ref().map(|dir| dir.to_str().unwrap_or("")).unwrap_or("");
                            Resp::Array(vec![
                                Resp::BulkString("dir".to_owned()),
                                Resp::BulkString(dir.to_owned())
                            ])
                        },
                        ServerType::Replica(_) => unimplemented!()
                    },
                    "dbfilename" => match &server_info.lock().unwrap().server_type {
                        ServerType::Master(state) => {
                            let db_filename = state.db_filename.as_deref().unwrap_or("");
                            Resp::Array(vec![
                                Resp::BulkString(config_key.to_owned()),
                                Resp::BulkString(db_filename.to_owned())
                            ])
                        },
                        ServerType::Replica(_) => unimplemented!()
                    },
                    _ => unimplemented!()
                }
            } else {
                unimplemented!()
            }
        }
    };
    stream.write_all(response.encode_to_string().as_bytes())?;
    Ok(())
}

fn handle_replica_commands(
    stream: TcpStream,
    server_info: Arc<Mutex<ServerStatus>>,
    replica_index: usize,
) -> anyhow::Result<()> {
    loop {
        let mut buf_reader = BufReader::new(stream.try_clone()?);
        let bytes = buf_reader.fill_buf()?;
        if bytes.is_empty() {
            return Ok(());
        }

        let remainder = match tokenize_bytes(bytes) {
            Ok((remainder, tokens)) => {
                println!("received from replica: {:?}", tokens);
                let command: RedisCommands = tokens.try_into()?;
                if let RedisCommands::ReplConf(commands::ReplConfMode::Ack(offset)) = command {
                    if let ServerType::Master(state) = &mut server_info.lock().unwrap().server_type {
                        if let Some(replica_data) = state.replicas_data.get_mut(replica_index) {
                            replica_data.latest_offset = offset as u64;
                        }
                    }
                }
                remainder
            }
            Err(err) => {
                println!("skip buffer since untokenizable: {}", err);
                bytes
            }
        };
        let consumed_bytes = bytes.len() - remainder.len();
        buf_reader.consume(consumed_bytes);
    }
}

fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}
