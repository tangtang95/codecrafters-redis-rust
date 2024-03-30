use anyhow::anyhow;

#[derive(PartialEq, Eq)]
pub enum Resp {
    Array(Vec<Resp>),
    BulkString(String),
    SimpleString(String),
    Integer(i64),
    NullBulkString
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
            },
            Resp::BulkString(string) => format!("${}\r\n{}\r\n", string.len(), string),
            Resp::SimpleString(string) => format!("+{}\r\n", string),
            Resp::Integer(num) => format!(":{}\r\n", num),
            Resp::NullBulkString => "$-1\r\n".to_string(),
        }
    }
}

pub fn tokenize(buffer: &str) -> anyhow::Result<(&str, Resp)> {
    let resp_type = buffer.get(0..1).ok_or(anyhow!("RESP type not found"))?;
    match resp_type {
       "*" => {
            let line = buffer.lines().next().ok_or(anyhow!("RESP next line not found"))?;
            let len = line.trim_start_matches('*').parse::<usize>()?;
            let mut vec: Vec<Resp> = Vec::new();
            let mut remainder = buffer.get(line.len()+2..).ok_or(anyhow!("RESP out of bounds"))?;
            for _ in 0..len {
                let (new_remainder, child_resp) = tokenize(remainder)?;
                vec.push(child_resp);
                remainder = new_remainder;
            }
            Ok((remainder, Resp::Array(vec)))
        },
       "$" => {
            let mut consumed_len = 0;
            let mut line_iter = buffer.lines();
            let line = line_iter.next().ok_or(anyhow!("RESP next line not found"))?;
            consumed_len += line.len() + 2;
            let len = line.trim_start_matches('$').parse::<usize>()?;

            let line = line_iter.next().ok_or(anyhow!("RESP next line not found"))?;
            if len != line.trim_end().len() {
                return Err(anyhow!("RESP bulk string len does not coincide"));
            }
            consumed_len += line.len() + 2;

            let remainder = buffer.get(consumed_len..).ok_or(anyhow!("RESP out of bounds"))?;
            Ok((remainder, Resp::BulkString(line.to_owned())))
        },
        ":" => {
            let line = buffer.lines().next().ok_or(anyhow!("RESP next line not found"))?;
            let integer = line.trim_start_matches(':').parse::<i64>()?;
            let remainder = buffer.get(line.len()+2..).ok_or(anyhow!("RESP out of bounds"))?;
            Ok((remainder, Resp::Integer(integer)))
        },
        _ => {
            println!("RESP type `{:?}` not implemented", resp_type);
            unimplemented!()
        }
    }
}
