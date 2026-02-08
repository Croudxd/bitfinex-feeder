use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt}; 

#[repr(C, packed)] 
struct Data {
    id: u64,    
    price: u64, 
    size: u64,  
    side: u8,   
    action: u8, 
}

#[tokio::main]
async fn main() 
{
    let url = "wss://api-pub.bitfinex.com/ws/2";

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected!");

    let (mut write, mut read) = ws_stream.split();

    let subscribe_json = r#"{ "event": "subscribe", "channel": "book", "pair": "tETHUSD", "prec": "R0" }"#;
    write.send(Message::Text(subscribe_json.into())).await.ok();

    while let Some(msg) = read.next().await
    {
        if let Ok(Message::Text(text)) = msg 
        {
            let parsed: serde_json::Value = match serde_json::from_str(&text) 
            {
                Ok(v) => v,
                Err(_) => continue, 
            };

            if !parsed.is_array() 
            {
                continue;
            }

            if parsed.is_object() 
            {
                continue;
            }
            
            let arr = parsed.as_array().unwrap();

            if arr.len() < 2 { continue; }
            
            if arr[1].is_string() 
            {
                continue; 
            }

            if let Some(inner_data) = arr[1].as_array() 
            {
                if inner_data.len() >= 3 
                {
                    let order_id = inner_data[0].as_u64().unwrap_or(0);
                    let price_f = inner_data[1].as_f64().unwrap_or(0.0);
                    let amount_f = inner_data[2].as_f64().unwrap_or(0.0);

                    let is_cancel = price_f == 0.0;
                    let side = if amount_f > 0.0 { 0 } else { 1 };

                    let packet = Data 
                    {
                        id: order_id,
                        price: (price_f * 100.0) as u64,
                        size: (amount_f.abs() * 1_000_000.0) as u64,
                        side,
                        action: if is_cancel { 1 } else { 0 },
                    };

                    println!("id: {}, price_f: {}, amount_f: {}", order_id, price_f, amount_f);
                }
            }
        }
    }
}

