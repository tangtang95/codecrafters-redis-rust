use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream}, thread,
};

use anyhow::anyhow;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Redis listening on port 6379");

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    let _ = handle_client(_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) -> anyhow::Result<()> {
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
                        handle_command(text.as_str().into(), args, &mut stream)?;
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

enum Resp {
    Array(Vec<Resp>),
    BulkString(String),
}

enum RedisCommands {
    Echo,
    Ping
}

impl From<&str> for RedisCommands {
    fn from(value: &str) -> Self {
        match value.to_lowercase().as_ref() {
            "echo" => RedisCommands::Echo,
            "ping" => RedisCommands::Ping,
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
            println!("{:?}", resp_type);
            unimplemented!()
        }
    }
}

fn handle_command(command: RedisCommands, args: &[Resp], stream: &mut TcpStream) -> anyhow::Result<()> {
    match command {
        RedisCommands::Echo => {
            match args.first() {
                Some(Resp::BulkString(text)) => {
                    let response = format!("+{}\r\n", text);
                    stream.write_all(response.as_bytes())?;
                    Ok(())
                },
                _ => Err(anyhow!("echo arg not found"))
            }
        },
        RedisCommands::Ping => {
            stream.write_all(b"+PONG\r\n")?;
            Ok(())
        }
    }
}
