use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream}, thread,
};

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
    let mut buf = [0u8; 256];

    loop {
        let bytes_read = stream.read(&mut buf)?;
        if bytes_read == 0 {
            return Ok(());
        }

        println!("received: {}", std::str::from_utf8(&buf)?);
        stream.write_all(b"+PONG\r\n")?;
    }
}
