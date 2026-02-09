use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt}; 
use std::fs::OpenOptions;
use memmap2::MmapMut;
use std::ptr;
use std::thread;
use std::sync::atomic::{fence, Ordering};
  
#[repr(C)] 
#[derive(Debug, Clone, Copy)]
struct Data {
    id: u64, 
    size: u64,  
    price: i32, 
    side: i8,   
    action: i8, 
    status: i8,
    _pad1: [u8; 1],
}

#[repr(C)]
struct Shared_memory_layout {
    write_idx: u64,
    _pad1: [u8; 56],
    read_idx: u64,
    _pad2: [u8; 56],
    buffer: [Data; 16384], 
}

#[tokio::main]
async fn main() 
{
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("/dev/shm/hft_ring")
        .expect("Failed to open SHM");

    let size = std::mem::size_of::<Shared_memory_layout>() as u64;
    file.set_len(size).unwrap();

    let mut mmap = unsafe { MmapMut::map_mut(&file).expect("Failed to map") };
    let shm = unsafe { &mut *(mmap.as_mut_ptr() as *mut Shared_memory_layout) };

    println!("Shared u64 Ready at /dev/shm/single_u64_shm");
    let url = "wss://api-pub.bitfinex.com/ws/2";

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected!");

    let (mut write, mut read) = ws_stream.split();

    let book_json = r#"{ "event": "subscribe", "channel": "book", "pair": "tETHUSD", "prec": "R0" }"#;
    write.send(Message::Text(book_json.into())).await.ok();

    let trade_json = r#"{ "event": "subscribe", "channel": "trades", "pair": "tETHUSD", "prec": "R0" }"#;
    write.send(Message::Text(trade_json.into())).await.ok();

    let mut book_channel_id: u64 = 0;
    let mut trade_channel_id: u64 = 0;
    while book_channel_id == 0 || trade_channel_id == 0 
    {
        let msg = read.next().await.expect("Stream closed unexpectedly").expect("Error reading msg");

        if let Message::Text(text) = msg 
        {
            let parsed: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();

            if let Some(event) = parsed.get("event") 
            {
                if event == "subscribed" 
                {
                    let channel = parsed["channel"].as_str().unwrap_or("");
                    let chan_id = parsed["chanId"].as_u64().unwrap_or(0);

                    if channel == "book" 
                    {
                        book_channel_id = chan_id;
                        println!("-> Book Channel Locked: {}", book_channel_id);
                    }
                    else if channel == "trades" 
                    {
                        trade_channel_id = chan_id;
                        println!("-> Trade Channel Locked: {}", trade_channel_id);
                    }
                }
            }
        }
    }
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
            let incoming_chan_id = arr[0].as_u64().unwrap_or(0);           
            let mut is_trade_msg = false;

            let payload = if arr[1].is_string() 
            {
                let msg_type = arr[1].as_str().unwrap_or("");
                    if msg_type == "hb" { continue; } // Skip Heartbeats
                    
                    // Trade Executions ("te") or Updates ("tu"). 
                    // Data is at arr[2]
                    if arr.len() < 3 { continue; }
                    is_trade_msg = true;
                    &arr[2]
            } 
            else 
            {
                    &arr[1]
            };

            let is_snapshot = payload.as_array()
                .and_then(|list| list.get(0))
                .map(|item| item.is_array())
                .unwrap_or(false);

            let orders_to_process: Vec<&serde_json::Value> = if is_snapshot 
            {
                payload.as_array().unwrap().iter().collect()
            } 
            else 
            {
                vec![payload]
            };
            for item in orders_to_process 
            {
                if let Some(inner_data) = item.as_array() 
                {
                    if inner_data.len() >= 3 
                    {
                        let order_id = inner_data[0].as_u64().unwrap_or(0);
                                
                                let (price_f, amount_f) = if is_trade_msg || incoming_chan_id == trade_channel_id 
                                {
                                     (inner_data[3].as_f64().unwrap_or(0.0), inner_data[2].as_f64().unwrap_or(0.0))
                                }
                                else 
                                {
                                     (inner_data[1].as_f64().unwrap_or(0.0), inner_data[2].as_f64().unwrap_or(0.0))
                                };

                        let is_cancel = price_f == 0.0;
                        let side = if amount_f > 0.0 { 0 } else { 1 };

                        let packet = Data {
                            id: order_id,
                            size: (amount_f.abs() * 1_000_000.0) as u64,
                            price: (price_f * 100.0) as i32,
                            side,
                            action: if is_cancel { 1 } else { 0 },
                            status: if incoming_chan_id == trade_channel_id { 1 } else { 0 },
                            _pad1: [0], 
                        };

                        ring_buffer(packet, shm);
                    }
                }
            }
        }
    }
}

fn ring_buffer (data: Data,  shm: &mut Shared_memory_layout)
{

    const BUFFER_CAPACITY: u64 = 16384; 

    loop 
    {
        let write_idx = unsafe { ptr::read_volatile(&shm.write_idx) };
        let read_idx  = unsafe { ptr::read_volatile(&shm.read_idx) };

        if write_idx - read_idx >= BUFFER_CAPACITY 
        {
            thread::yield_now(); 
            continue;
        }

        let slot = (write_idx % BUFFER_CAPACITY) as usize;

        unsafe 
        {
            ptr::write_volatile(&mut shm.buffer[slot], data);
        }

        fence(Ordering::Release);

        unsafe 
        {
            ptr::write_volatile(&mut shm.write_idx, write_idx + 1);
        }

        break;
    }
}
