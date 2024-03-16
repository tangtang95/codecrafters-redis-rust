use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream}, thread, collections::HashMap, sync::{Arc, Mutex},
};

use anyhow::anyhow;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Redis listening on port 6379");

    let redis_map = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let redis_map = redis_map.clone();
                thread::spawn(move || {
                    let _ = handle_client(_stream, redis_map);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, redis_map: Arc<Mutex<HashMap<String, String>>>) -> anyhow::Result<()> {
    loop {
        let mut bytes = [0u8; 512];
        let bytes_read = stream.read(&mut bytes)?;
        if bytes_read == 0 {
            return Ok(());
        }

        let buf = String::from_utf8(bytes.to_vec())?;
        println!("received: {:?}", buf.trim_end_matches('\0'));
        let (_, commands) = parse_resp(buf.trim_end_matches('\0'))?;
        match &commands {
            Resp::Array(array) => {
                match array.first() {
                    Some(Resp::BulkString(text)) => {
                        let args = array.get(1..).ok_or(anyhow!("Should not go out of bounds"))?;
                        handle_command(text.as_str().into(), args, &mut stream, redis_map.clone())?;
                    },
                    Some(_) => unimplemented!(),
                    None => unimplemented!()
                }
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

#[derive(PartialEq)]
enum Resp<'a> {
    Array(Vec<Resp<'a>>),
    BulkString(String),
    SimpleString(&'a str),
    NullBulkString
}

impl<'a> Resp<'a> {
    pub fn encode_to_string(&self) -> String {
        match self {
            Resp::Array(_) => todo!(),
            Resp::BulkString(string) => format!("${}\r\n{}\r\n", string.len(), string),
            Resp::SimpleString(string) => format!("+{}\r\n", string),
            Resp::NullBulkString => "$-1\r\n".to_string(),
        }
    }
}

enum RedisCommands {
    Echo,
    Ping,
    Set,
    Get
}

impl From<&str> for RedisCommands {
    fn from(value: &str) -> Self {
        match value.to_lowercase().as_ref() {
            "echo" => RedisCommands::Echo,
            "ping" => RedisCommands::Ping,
            "set" => RedisCommands::Set,
            "get" => RedisCommands::Get,
            _ => unimplemented!()
        }
    }
}

fn parse_resp(resp: &str) -> anyhow::Result<(&str, Resp)> {
    let resp_type = resp.get(0..1).ok_or(anyhow!("RESP type not found"))?;
    match resp_type {
       "*" => {
            let line = resp.lines().next().ok_or(anyhow!("RESP next line not found"))?;
            let len = line.trim_start_matches('*').parse::<usize>()?;
            let mut vec: Vec<Resp> = Vec::new();
            let mut remainder = resp.get(line.len()+2..).ok_or(anyhow!("RESP out of bounds"))?;
            for _ in 0..len {
                let (new_remainder, child_resp) = parse_resp(remainder)?;
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
        _ => {
            println!("RESP type `{:?}` not implemented", resp_type);
            unimplemented!()
        }
    }
}

fn handle_command(command: RedisCommands, args: &[Resp], stream: &mut TcpStream, redis_map: Arc<Mutex<HashMap<String, String>>>) -> anyhow::Result<()> {
    match command {
        RedisCommands::Echo => {
            match args.first() {
                Some(Resp::BulkString(text)) => {
                    let response = Resp::SimpleString(text);
                    stream.write_all(response.encode_to_string().as_bytes())?;
                    Ok(())
                },
                _ => Err(anyhow!("echo arg not found"))
            }
        },
        RedisCommands::Ping => {
            let pong = Resp::SimpleString("PONG");
            stream.write_all(pong.encode_to_string().as_bytes())?;
            Ok(())
        },
        RedisCommands::Set => {
            match args.get(0..2) {
                Some([key, value]) => {
                    let Resp::BulkString(key) = key else { return Err(anyhow!("set command invalid key")) };
                    let Resp::BulkString(value) = value else { return Err(anyhow!("set command invalid key")) };
                    let mut map = redis_map.lock().unwrap();
                    map.insert(key.to_owned(), value.to_owned());
                    let ok = Resp::SimpleString("OK");
                    stream.write_all(ok.encode_to_string().as_bytes())?;
                    Ok(())
                },
                _ => Err(anyhow!("set command needs at least 2 arguments")),
            }
        },
        RedisCommands::Get => {
            match args.first() {
                Some(Resp::BulkString(key)) => {
                    let value = redis_map.lock().unwrap().get(key).map(|k| k.to_owned());
                    if let Some(value) = value {
                        let value = Resp::BulkString(value);
                        stream.write_all(value.encode_to_string().as_bytes())?;
                    } else {
                        let null = Resp::NullBulkString;
                        stream.write_all(null.encode_to_string().as_bytes())?;
                    }
                    Ok(())
                },
                _ => Err(anyhow!("echo arg not found"))
            }
        },
    }
}
