use anyhow::anyhow;

#[derive(Debug, PartialEq, Eq)]
pub enum Resp {
    Array(Vec<Resp>),
    BulkString(String),
    SimpleString(String),
    Integer(i64),
    NullBulkString,
    Empty,
}

impl Resp {
    pub fn encode_to_string(&self) -> String {
        match self {
            Resp::Array(vector) => {
                let mut encoded = format!("*{}\r\n", vector.len());
                for val in vector {
                    encoded += &val.encode_to_string()
                }
                encoded
            }
            Resp::BulkString(string) => format!("${}\r\n{}\r\n", string.len(), string),
            Resp::SimpleString(string) => format!("+{}\r\n", string),
            Resp::Integer(num) => format!(":{}\r\n", num),
            Resp::NullBulkString => "$-1\r\n".to_string(),
            Resp::Empty => String::new(),
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        match self {
            Resp::Array(vector) => {
                let mut encoded = [b"*", vector.len().to_string().as_bytes(), b"\r\n"].concat();
                for val in vector {
                    encoded = [encoded, val.encode_to_bytes()].concat();
                }
                encoded
            }
            Resp::BulkString(string) => [
                b"$",
                string.len().to_string().as_bytes(),
                b"\r\n",
                string.as_bytes(),
                b"\r\n",
            ]
            .concat(),
            Resp::SimpleString(string) => [b"+", string.as_bytes(), b"\r\n"].concat(),
            Resp::Integer(num) => [b":", num.to_string().as_bytes(), b"\r\n"].concat(),
            Resp::NullBulkString => b"$-1\r\n".to_vec(),
            Resp::Empty => vec![],
        }
    }
}

pub fn tokenize_bytes(buffer: &[u8]) -> anyhow::Result<(&[u8], Resp)> {
    let value_type = buffer.first().ok_or(anyhow!("RESP type not found"))?;
    match value_type {
        b'*' => {
            let (mut remainder, line_bytes) = read_next_line(buffer)?;
            let len = String::from_utf8(line_bytes[1..].to_vec())?.parse::<usize>()?;
            let mut vec: Vec<Resp> = Vec::new();
            for _ in 0..len {
                let (new_remainder, child_resp) = tokenize_bytes(remainder)?;
                vec.push(child_resp);
                remainder = new_remainder;
            }
            Ok((remainder, Resp::Array(vec)))
        }
        b'$' => {
            let (remainder, line_bytes) = read_next_line(buffer)?;
            let len = String::from_utf8(line_bytes[1..].to_vec())?.parse::<usize>()?;
            let (remainder, line_bytes) = read_next_line(remainder)?;
            let text = String::from_utf8(line_bytes[..].to_vec())?;
            if len != text.len() {
                return Err(anyhow!("RESP bulk string len does not coincide"));
            }
            Ok((remainder, Resp::BulkString(text.to_owned())))
        }
        b':' => {
            let (remainder, line_bytes) = read_next_line(buffer)?;
            let integer = String::from_utf8(line_bytes[1..].to_vec())?.parse::<i64>()?;
            Ok((remainder, Resp::Integer(integer)))
        }
        b'+' => {
            let (remainder, line_bytes) = read_next_line(buffer)?;
            let text = String::from_utf8(line_bytes[1..].to_vec())?;
            Ok((remainder, Resp::SimpleString(text.to_string())))
        }
        _ => {
            println!("RESP type `{}` not implemented", char::from(*value_type));
            unimplemented!()
        }
    }
}

pub fn read_next_line(buffer: &[u8]) -> anyhow::Result<(&[u8], &[u8])> {
    let (next_rn_idx, next_line_idx) = match buffer.windows(2).position(|bytes| bytes == b"\r\n") {
        Some(index) => (index, index + 2),
        None => (buffer.len(), buffer.len()),
    };
    let line_bytes = buffer.get(..next_rn_idx).ok_or(anyhow!("RESP next line not found"))?;
    let remainder = buffer.get(next_line_idx..).ok_or(anyhow!("RESP remainder not found"))?;
    Ok((remainder, line_bytes))
}
