use std::{io::Write, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Redis listening on port 6379");

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                _stream.write_all("+PONG\r\n".as_bytes()).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
