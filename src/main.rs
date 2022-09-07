use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::SystemTime;

use futures_util::{SinkExt, StreamExt};
use hmac_sha256::HMAC;
use rust_decimal::prelude::RoundingStrategy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{from_str, to_string};
use time::OffsetDateTime;
use tokio::sync::{broadcast, mpsc, Semaphore, SemaphorePermit};
use tokio::task::{yield_now, JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

// DD: Feel free to adjust the parameters up to you.

const MARKET_WS_URL: &'static str = "wss://stream.crypto.com/v2/market";
const USER_WS_URL: &'static str = "wss://stream.crypto.com/v2/user";
const API_KEY: &'static str = ""; // insert your api key here
const SECRET_KEY: &'static str = ""; // insert your secret key here
const GAIN_THRESHOLD: Decimal = dec!(1.001); // Execute chains having gain only above this threshold (fees are taken into account)
const DAY_VOLUME_THRESHOLD: f64 = 3500.0; // Execute chains only with trading pairs having more volume than this threshold
const CHAINS_APPROX_FRACTION: f32 = 1.0; // Coefficient to work only with a part of all built chains.

// DD: Most of the chains start on just a few currencies line USDT, USDC, BTC.
// Doesn't make sense to look for other ones as they don't have enough volume
// for you to execute the chains anyway.

const STARTING_CURRENCIES: [&str; 3] = ["USDT", "USDC", "BTC"];
const STARTING_BALANCE_USDT: Decimal = dec!(2.0);
const STARTING_BALANCE_USDC: Decimal = dec!(2.0);
const STARTING_BALANCE_BTC: Decimal = dec!(0.0001);
const TRADING_FEE: Decimal = dec!(0.99925);

const USER_MPSC_REQUEST_CAPACITY: usize = 10;
const USER_BROADCAST_RESPONSE_CAPACITY: usize = 2;

const MARKET_MPSC_REQUEST_CAPACITY: usize = 10;
const MARKET_BROADCAST_RESPONSE_CAPACITY: usize = 32;
const MARKET_BROADCAST_DISPATCH_CAPACITY: usize = 32;

const ARB_EXECUTOR_ORDER_TIMEOUT: u64 = 3000;
const ARB_EXECUTOR_PENDING_TIMEOUT: u64 = 180000;

// Turn on/off the actual trading.
const RESEARCH_MODE: bool = false;

// Tools to perform get requests from exchange

#[derive(Debug, Deserialize, Clone)]
pub struct Response<T> {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub method: String,
    #[serde(deserialize_with = "str_or_i64")]
    pub code: i64,
    #[serde(default)]
    pub result: Option<T>,
}

fn str_or_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrU64<'a> {
        Str(&'a str),
        I64(i64),
    }

    Ok(match StrOrU64::deserialize(deserializer)? {
        StrOrU64::Str(v) => v.parse().unwrap_or(0), // Ignoring parsing errors
        StrOrU64::I64(v) => v,
    })
}

async fn get_exc<T: DeserializeOwned + Default>(client: &reqwest::Client, url: &str) -> Result<T, Box<dyn Error>> {
    let response = client.get(url).send().await?;
    let response_body: String = response.text().await?;
    let response_body_serialized: Response<T> = from_str(response_body.as_str())?;
    Ok(response_body_serialized.result.unwrap())
}

// Initial possible chains exploration and management

#[derive(Debug, Deserialize, Default)]
pub struct TickersData {
    pub data: Vec<TickerData>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Instruments {
    pub instruments: Vec<Instrument>,
}

#[derive(Debug, Clone)]
pub struct ArbitrageChain {
    pub orders: [Order; 3],
    pub is_buys: [bool; 3],
    pub quantity_precisions: [usize; 3],
    pub starting_currency: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Instrument {
    pub instrument_name: String,
    pub quote_currency: String,
    pub base_currency: String,
    pub price_decimals: usize,
    pub quantity_decimals: usize,
    pub max_quantity: String,
    pub min_quantity: String,
}

impl Instruments {
    pub fn filter_day_vol(&mut self, tickers_data: Vec<TickerData>, day_volume_threshold: f64) {
        // DD: Filtering day volumes in some way is necessary due to pairs
        // with low volumes always contributing to some beefy gains but
        // not being actually executable (i.e. you can't immediately execute
        // deals on those pairs at the strict price points you've set)
        let mut instruments_filtered: Vec<Instrument> = Vec::new();
        for instrument in self.instruments.iter() {
            for ticker in tickers_data.iter() {
                if ticker.instrument.as_ref().unwrap() == &instrument.instrument_name && ticker.vol_traded_day_usd > day_volume_threshold {
                    instruments_filtered.push(instrument.clone());
                }
            }
        }
        self.instruments = instruments_filtered;
    }

    pub fn get_chains(&self, starting_currencies: Vec<&str>, approx_fraction: f32) -> (Vec<ArbitrageChain>, Vec<String>) {
        // DD: Computing the chains the dumbest way possible as it is
        // not a performance-sensitive part of the program. Returning
        // back the actual orders that have to be sent to the exchange.
        let mut arbitrage_chains: Vec<ArbitrageChain> = Vec::new();
        let mut instruments_all_chains: HashSet<String> = HashSet::new();

        for starting_currency in starting_currencies {
            for first_instrument in self
                .instruments
                .iter()
                .filter(|v| v.base_currency == starting_currency || v.quote_currency == starting_currency)
            {
                let mut new_chain: [String; 3] = ["".to_owned(), "".to_owned(), "".to_owned()];
                if first_instrument.quote_currency == starting_currency {
                    new_chain[0] = first_instrument.quote_currency.clone();
                    new_chain[1] = first_instrument.base_currency.clone();
                } else if first_instrument.base_currency == starting_currency {
                    new_chain[0] = first_instrument.base_currency.clone();
                    new_chain[1] = first_instrument.quote_currency.clone();
                } else {
                    panic!("starting instrument has to contain starting currency (most likely starting currency filtering is broken)");
                }

                for second_instrument in self.instruments.iter().filter(|v| v.instrument_name != first_instrument.instrument_name) {
                    let mut new_chain_second_level = new_chain.clone();
                    let asset_to_add: String;
                    if second_instrument.quote_currency == new_chain_second_level[1] {
                        asset_to_add = second_instrument.base_currency.clone();
                    } else if second_instrument.base_currency == new_chain_second_level[1] {
                        asset_to_add = second_instrument.quote_currency.clone();
                    } else {
                        continue;
                    }

                    let possible_third_instrument_name = format!("{}_{}", starting_currency, asset_to_add);
                    let possible_third_instrument_name_rev = format!("{}_{}", asset_to_add, starting_currency);

                    for third_instrument in self
                        .instruments
                        .iter()
                        .filter(|v| v.base_currency == starting_currency || v.quote_currency == starting_currency)
                    {
                        if (possible_third_instrument_name == third_instrument.instrument_name
                            || possible_third_instrument_name_rev == third_instrument.instrument_name)
                            && rand::random::<f32>() < approx_fraction
                        {
                            new_chain_second_level[2] = asset_to_add;

                            let chain_instruments: [Instrument; 3] = [first_instrument.clone(), second_instrument.clone(), third_instrument.clone()];
                            let mut sides: [String; 3] = ["".to_owned(), "".to_owned(), "".to_owned()];
                            let mut is_buys = [false; 3];
                            for idx in 0..3 {
                                instruments_all_chains.insert(chain_instruments[idx].instrument_name.clone());
                                if chain_instruments[idx].instrument_name.starts_with(&new_chain_second_level[idx]) {
                                    is_buys[idx] = false;
                                    sides[idx] = "SELL".to_owned();
                                } else {
                                    is_buys[idx] = true;
                                    sides[idx] = "BUY".to_owned();
                                }
                            }
                            let orders: [Order; 3] = core::array::from_fn(|i| Order {
                                instrument_name: chain_instruments[i].instrument_name.clone(),
                                side: sides[i].clone(),
                                type_: "LIMIT".to_owned(),
                                price: None,
                                quantity: None,
                                time_in_force: None,
                            });
                            let quantity_precisions: [usize; 3] = core::array::from_fn(|i| chain_instruments[i].quantity_decimals);

                            let arbitrage_chain = ArbitrageChain {
                                orders: orders,
                                is_buys: is_buys,
                                quantity_precisions: quantity_precisions,
                                starting_currency: starting_currency.to_owned(),
                            };

                            arbitrage_chains.push(arbitrage_chain);
                            break;
                        }
                    }
                    if new_chain_second_level[2] != "" {
                        continue;
                    }
                }
            }
        }
        let instruments_all_chains_vec: Vec<String> = instruments_all_chains.into_iter().collect();
        (arbitrage_chains, instruments_all_chains_vec)
    }
}

// Structs to serialize WebSocket request

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "method")]
pub enum RequestWebSocket {
    #[serde(rename = "public/respond-heartbeat")]
    PublicRespondHeartbeat { id: u64 },
    #[serde(rename = "subscribe")]
    Subscribe { id: u64, nonce: u64, params: RequestWebSocketParams },
    #[serde(rename = "public/auth")]
    Auth { id: u64, api_key: String, sig: String, nonce: u64 },
    #[serde(rename = "private/create-order")]
    CreateOrder { id: i64, nonce: u64, params: Order },
    #[serde(rename = "private/cancel-all-orders")]
    CancelAllOrders { id: i64, nonce: u64, params: OrderCancellation },
}

#[derive(Serialize, Debug, Clone)]
pub enum RequestWebSocketParams {
    #[serde(rename = "channels")]
    Channels(Vec<String>),
}

#[derive(Debug, Serialize, Clone)]
pub struct Order {
    pub instrument_name: String,
    pub side: String,
    #[serde(rename(serialize = "type"))]
    pub type_: String,
    #[serde(with = "rust_decimal::serde::float_option")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub quantity: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct OrderCancellation {
    pub instrument_name: String,
}

// Structs to deserialize WebSocket responses

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "method")]
pub enum ResponseWebSocket {
    #[serde(rename = "public/heartbeat")]
    PublicHeartbeat { id: Option<u64>, code: Option<i64> },
    #[serde(rename = "public/auth")]
    Auth { id: u64, code: i64 },
    #[serde(rename = "subscribe")]
    Subscribe {
        id: Option<i64>,
        code: Option<i64>,
        result: Option<ResponseResult>,
    },
    #[serde(rename = "private/create-order")]
    CreateOrder {
        id: i64,
        code: i64,
        result: Option<OrderCreationResult>,
    },
    #[serde(rename = "private/cancel-all-orders")]
    CancelAllOrders { id: i64, code: i64 },
}

#[derive(Deserialize, Debug, Clone)]
pub struct OrderCreationResult {
    pub order_id: String,
    pub client_oid: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "channel")]
pub enum ResponseResult {
    #[serde(rename = "ticker")]
    Ticker { instrument_name: String, data: Vec<TickerData> },
    #[serde(rename = "user.order")]
    UserOrder { instrument_name: String, data: Vec<UserOrderData> },
}

#[derive(Deserialize, Debug, Clone)]
pub struct TickerData {
    #[serde(alias = "i")]
    pub instrument: Option<String>,
    #[serde(alias = "vv")]
    pub vol_traded_day_usd: f64,
    #[serde(alias = "a")]
    #[serde(with = "rust_decimal::serde::float")]
    pub price_lastest_trade: Decimal,
    #[serde(alias = "b")]
    #[serde(with = "rust_decimal::serde::float")]
    pub price_bid_best: Decimal,
    #[serde(alias = "k")]
    #[serde(with = "rust_decimal::serde::float")]
    pub price_ask_best: Decimal,
}

#[derive(Deserialize, Debug, Clone)]
pub struct UserOrderData {
    pub status: String,
    pub side: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub quantity: Decimal,
    pub order_id: String,
    pub client_oid: String,
    pub create_time: u64,
    pub update_time: u64,
    #[serde(alias = "type")]
    pub type_: String,
    pub instrument_name: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub cumulative_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub cumulative_value: Decimal,
    pub avg_price: f64,
    pub fee_currency: String,
    pub time_in_force: String,
    pub reason: Option<u64>,
}

// DD: UserWebSocket allows to send the orders to the its request channel.
// All the broadcast channel subscribers are going to receive all the
// updates on the current orders, which they are going to filter out themselves.

pub struct UserWebSocket {
    pub mpsc_request_sender: mpsc::Sender<RequestWebSocket>,
    pub broadcast_response_sender: broadcast::Sender<ResponseWebSocket>,
    pub request_task_handle: JoinHandle<()>,
    pub response_task_handle: JoinHandle<()>,
}

impl UserWebSocket {
    async fn new() -> UserWebSocket {
        let (ws_stream, _) = connect_async(USER_WS_URL.to_owned()).await.expect("failed to connect to user websocket");
        let (mut ws_writer, mut ws_reader) = ws_stream.split();

        let (mpsc_request_sender, mut mpsc_request_receiver): (mpsc::Sender<RequestWebSocket>, mpsc::Receiver<RequestWebSocket>) =
            mpsc::channel(USER_MPSC_REQUEST_CAPACITY);
        let request_task_handle = tokio::spawn(async move {
            while let Some(message) = mpsc_request_receiver.recv().await {
                let request_body_serialized = to_string(&message).unwrap();
                let request_message = Message::text(request_body_serialized);
                ws_writer.send(request_message).await.unwrap();
            }
        });

        let mpsc_request_sender_clone = mpsc_request_sender.clone();
        let (broadcast_response_sender, _): (broadcast::Sender<ResponseWebSocket>, broadcast::Receiver<ResponseWebSocket>) =
            broadcast::channel(USER_BROADCAST_RESPONSE_CAPACITY);
        let broadcast_response_sender_clone = broadcast_response_sender.clone();
        let response_task_handle = tokio::spawn(async move {
            while let Some(message) = ws_reader.next().await {
                let data = message.unwrap().into_text().unwrap();
                let response_ws: Option<ResponseWebSocket> = from_str(&data).ok();
                match response_ws {
                    Some(ResponseWebSocket::PublicHeartbeat { id, .. }) => {
                        let respond_heartbeat_request = RequestWebSocket::PublicRespondHeartbeat { id: id.unwrap() };
                        mpsc_request_sender_clone.try_send(respond_heartbeat_request).unwrap();
                    }
                    Some(rws) => {
                        broadcast_response_sender.send(rws);
                    }
                    None => {
                        continue;
                    }
                }
            }
        });

        UserWebSocket {
            mpsc_request_sender: mpsc_request_sender,
            broadcast_response_sender: broadcast_response_sender_clone,
            request_task_handle: request_task_handle,
            response_task_handle: response_task_handle,
        }
    }

    async fn auth(&self) {
        let id = rand::random::<u16>() as u64;
        let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        let payload_str = format!("{}{}{}{}{}", "public/auth", id, API_KEY, "".to_owned(), nonce);
        let sig = hex::encode(HMAC::mac(&payload_str, SECRET_KEY));
        let request_auth = RequestWebSocket::Auth {
            id: id,
            api_key: API_KEY.to_owned(),
            sig: sig,
            nonce: nonce,
        };
        let mut broadcast_response_receiver = self.broadcast_response_sender.subscribe();
        self.mpsc_request_sender.send(request_auth).await.unwrap();
        match broadcast_response_receiver.recv().await.unwrap() {
            ResponseWebSocket::Auth {
                id: response_id,
                code: response_code,
                ..
            } => {
                if !(id == response_id && response_code == 0) {
                    panic!("failed to authenticate to user websocket");
                }
            }
            _ => panic!("received something else instead of user ws authentication confirmation"),
        }
    }

    async fn subscribe(&self, instruments: &Vec<String>, channel: &str) {
        let channels: Vec<String>;
        match channel {
            "user.order" => channels = instruments.iter().map(|v| format!("user.order.{}", v)).collect(),
            _ => panic!("unknown channel type"),
        }
        let id = rand::random::<u16>() as u64;
        let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        let request_subscribe = RequestWebSocket::Subscribe {
            id: id,
            nonce: nonce,
            params: RequestWebSocketParams::Channels(channels),
        };
        let mut broadcast_response_receiver = self.broadcast_response_sender.subscribe();
        self.mpsc_request_sender.send(request_subscribe).await.unwrap();
        match broadcast_response_receiver.recv().await.unwrap() {
            ResponseWebSocket::Subscribe {
                id: _, code: response_code, ..
            } => {
                if response_code != Some(0) {
                    panic!("failed to subscribe to user websocket");
                }
            }
            _ => panic!("received something else instead of user ws subscription confirmation"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketUpdate {
    pub instrument_name: String,
    pub price_ask_best: Decimal,
    pub price_bid_best: Decimal,
}

// DD: MarketWebSocket allows currently allows only to subscribe to ticker channel.
// instrument_channels field stores a HashMap with instrument -> broadcast channel
// pairs, which allows users to subscribe only to the channels that they need.

pub struct MarketWebSocket {
    pub mpsc_request_sender: mpsc::Sender<RequestWebSocket>,
    pub broadcast_response_sender: broadcast::Sender<ResponseWebSocket>,
    pub request_task_handle: JoinHandle<()>,
    pub stream_task_handle: JoinHandle<()>,
    pub dispatch_task_handle: Option<JoinHandle<()>>,
    pub instrument_channels: Option<HashMap<String, broadcast::Sender<MarketUpdate>>>,
}

impl MarketWebSocket {
    async fn new() -> MarketWebSocket {
        let (ws_stream, _) = connect_async(MARKET_WS_URL.to_owned())
            .await
            .expect("failed to connect to market websocket");
        let (mut ws_writer, mut ws_reader) = ws_stream.split();

        let (mpsc_request_sender, mut mpsc_request_receiver): (mpsc::Sender<RequestWebSocket>, mpsc::Receiver<RequestWebSocket>) =
            mpsc::channel(MARKET_MPSC_REQUEST_CAPACITY);
        let request_task_handle = tokio::spawn(async move {
            while let Some(message) = mpsc_request_receiver.recv().await {
                let request_body_serialized = to_string(&message).unwrap();
                let request_message = Message::text(request_body_serialized);
                ws_writer.send(request_message).await.unwrap();
            }
        });

        let mpsc_request_sender_clone = mpsc_request_sender.clone();
        let (broadcast_response_sender, _): (broadcast::Sender<ResponseWebSocket>, broadcast::Receiver<ResponseWebSocket>) =
            broadcast::channel(MARKET_BROADCAST_RESPONSE_CAPACITY);
        let broadcast_response_sender_clone = broadcast_response_sender.clone();
        let stream_task_handle = tokio::spawn(async move {
            // DD: ws_reader is almost always ready, so to get a healthy work distribution
            // between tasks I had to put yields all over the place.
            while let Some(message) = ws_reader.next().await {
                let data = message.unwrap().into_text().unwrap();
                let response_ws: Option<ResponseWebSocket> = from_str(&data).ok();
                yield_now().await;
                match response_ws {
                    Some(ResponseWebSocket::PublicHeartbeat { id, .. }) => {
                        let respond_heartbeat_request = RequestWebSocket::PublicRespondHeartbeat { id: id.unwrap() };
                        mpsc_request_sender.try_send(respond_heartbeat_request).unwrap();
                        yield_now().await;
                    }
                    Some(rws) => {
                        broadcast_response_sender.send(rws);
                        yield_now().await;
                    }
                    None => {
                        yield_now().await;
                        continue;
                    }
                }
            }
        });

        MarketWebSocket {
            mpsc_request_sender: mpsc_request_sender_clone,
            broadcast_response_sender: broadcast_response_sender_clone,
            request_task_handle: request_task_handle,
            stream_task_handle: stream_task_handle,
            dispatch_task_handle: None,
            instrument_channels: None,
        }
    }

    async fn subscribe(&mut self, instruments: &Vec<String>, channel: &str) {
        let channels: Vec<String>;
        match channel {
            "ticker" => channels = instruments.iter().map(|v| format!("ticker.{}", v)).collect(),
            _ => panic!("market subscribe: unknown channel type"),
        }

        let mut instrument_channels: HashMap<String, broadcast::Sender<MarketUpdate>> = HashMap::with_capacity(instruments.len());
        for instrument_name in instruments.iter() {
            let (broadcast_response_sender_dispatch, _): (broadcast::Sender<MarketUpdate>, broadcast::Receiver<MarketUpdate>) =
                broadcast::channel(MARKET_BROADCAST_DISPATCH_CAPACITY);
            instrument_channels.insert(instrument_name.to_string(), broadcast_response_sender_dispatch);
        }
        let instrument_channels_clone_dispatch = instrument_channels.clone();
        let mut broadcast_response_receiver_instance_dispatch = self.broadcast_response_sender.subscribe();
        let dispatch_task_handle = tokio::spawn(async move {
            while let Ok(update) = broadcast_response_receiver_instance_dispatch.recv().await {
                match update {
                    ResponseWebSocket::Subscribe {
                        id: _,
                        code: _,
                        result: Some(ResponseResult::Ticker { instrument_name, data, .. }),
                    } => {
                        let instrument_channel = &instrument_channels_clone_dispatch[&instrument_name];
                        // DD: Price of the latest trade should be taken into account if it's not
                        // between best bid and ask prices. This way you should get more orders filled.
                        let mut price_ask_best = data[0].price_ask_best;
                        let mut price_bid_best = data[0].price_bid_best;
                        if data[0].price_lastest_trade > data[0].price_ask_best {
                            price_ask_best = data[0].price_lastest_trade;
                        }
                        if data[0].price_lastest_trade < data[0].price_bid_best {
                            price_bid_best = data[0].price_lastest_trade;
                        }
                        let market_update = MarketUpdate {
                            instrument_name: instrument_name,
                            price_ask_best: price_ask_best,
                            price_bid_best: price_bid_best,
                        };
                        instrument_channel.send(market_update);
                    }
                    ResponseWebSocket::Subscribe {
                        id: _,
                        code: _,
                        result: None,
                    } => continue,
                    _ => panic!("market dispatch: received unknown message type"),
                }
            }
        });
        self.dispatch_task_handle = Some(dispatch_task_handle);
        self.instrument_channels = Some(instrument_channels);

        let id = rand::random::<u16>() as u64;
        let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        let request_subscribe = RequestWebSocket::Subscribe {
            id: id,
            nonce: nonce,
            params: RequestWebSocketParams::Channels(channels),
        };
        let mut broadcast_response_receiver_instance_subscription = self.broadcast_response_sender.subscribe();
        self.mpsc_request_sender.send(request_subscribe).await.unwrap();
        match broadcast_response_receiver_instance_subscription.recv().await.unwrap() {
            ResponseWebSocket::Subscribe {
                id: _, code: response_code, ..
            } => {
                if response_code != Some(0) {
                    panic!("market subscribe: failed to subscribe to user websocket");
                }
            }
            _ => panic!("market subscribe: received something else instead of user ws subscription confirmation"),
        }
    }
}

// DD: The main function.
//
// 1. Initial stage: filter out the instruments with low volumes,
// build the chains, get price and quantity precisions, subscribe
// to all the websocket channels that we need.
// 2. Main stage: Spawn a dedicated task for each chain.
// 3. Arbitrage task.
//   3.1. The most convenient way to manage the opportunities discovery
//        and execution I came up with is to write a simple state machine.
//        There are a lot of scenarios that happen during the trading, but
//        they can be easily grouped into a few states. This approach
//        greatly simplifies the control flow and is easy to manage as we can
//        clearly see and define all the rules of how the executor changes its
//        state.
//   3.2. The state is known at the start and at the end of each iteration of
//        the infinite loop.
//   3.3. Semaphore with capacity of 1 is used to make sure we only execute
//        one chain at a time - basically this is a simple replacement for
//        asset allocation mechanism, which works because asset allocation is
//        about scaling the work of the bot, but you obviously don't need any
//        scaling it when the profits are less than or equal to zero.

pub enum ArbExecutorState<'a> {
    Pending(u64),
    Collecting,
    CalculationReady,
    ExecutionReady(u8, SemaphorePermit<'a>, bool),        // step, permit, cancellation flag
    ExecutionStop(SemaphorePermit<'a>, bool),             // permit, "suspend execution" flag
    ExecutionPending(u8, SemaphorePermit<'a>, bool, u64), // step, permit, cancellation flag, millisecond timestamp
}

#[tokio::main]
async fn main() {
    // DD: The preparation part is more or less self-explanatory according to
    // logging messages.
    let client = reqwest::Client::new();
    print!("getting exchange info... ");
    let mut instr: Instruments = get_exc(&client, "https://api.crypto.com/v2/public/get-instruments").await.unwrap();
    let ticker_data: TickersData = get_exc(&client, "https://api.crypto.com/v2/public/get-ticker").await.unwrap();
    print!("done\n");
    io::stdout().flush().unwrap();

    print!("building chains... ");
    instr.filter_day_vol(ticker_data.data, DAY_VOLUME_THRESHOLD);
    let (arbitrage_chains, instruments_all_chains_vec) = instr.get_chains(STARTING_CURRENCIES.to_vec(), CHAINS_APPROX_FRACTION);
    print!(
        "done, built {} chains that use {} instruments\n",
        arbitrage_chains.len(),
        instruments_all_chains_vec.len()
    );
    io::stdout().flush().unwrap();

    print!("attempting user websocket handshake... ");
    let user_ws = UserWebSocket::new().await;
    user_ws.auth().await;
    print!("done\n");
    io::stdout().flush().unwrap();

    print!("attempting user websocket subscription and execute arbitrage task spawn... ");
    user_ws.subscribe(&instruments_all_chains_vec, "user.order").await;
    print!("done\n");

    io::stdout().flush().unwrap();

    print!("attempting market websocket handshake... ");
    let mut market_ws = MarketWebSocket::new().await;
    print!("done\n");
    io::stdout().flush().unwrap();

    print!("attempting main websocket subscription... ");
    market_ws.subscribe(&instruments_all_chains_vec, "ticker").await;
    print!("done\n");
    io::stdout().flush().unwrap();

    println!("spawning tasks and starting the processing");

    let arb_chain_execution_semaphore = Arc::new(Semaphore::new(1));
    let mut tasks: Vec<_> = Vec::new();
    for mut arbitrage_chain in arbitrage_chains.into_iter() {
        let mut user_broadcast_response_receiver = user_ws.broadcast_response_sender.subscribe();
        let user_mpsc_request_sender = user_ws.mpsc_request_sender.clone();
        let mut market_instrument_channel_receiver_0: broadcast::Receiver<MarketUpdate> =
            market_ws.instrument_channels.as_ref().unwrap()[&arbitrage_chain.orders[0].instrument_name].subscribe();
        let mut market_instrument_channel_receiver_1: broadcast::Receiver<MarketUpdate> =
            market_ws.instrument_channels.as_ref().unwrap()[&arbitrage_chain.orders[1].instrument_name].subscribe();
        let mut market_instrument_channel_receiver_2: broadcast::Receiver<MarketUpdate> =
            market_ws.instrument_channels.as_ref().unwrap()[&arbitrage_chain.orders[2].instrument_name].subscribe();
        let arb_chain_execution_semaphore_instance = arb_chain_execution_semaphore.clone();

        let handle = tokio::spawn(async move {
            let time_format = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap();
            let current_balance: Decimal;
            if arbitrage_chain.starting_currency == "USDT".to_owned() {
                current_balance = STARTING_BALANCE_USDT;
            } else if arbitrage_chain.starting_currency == "USDC".to_owned() {
                current_balance = STARTING_BALANCE_USDC;
            } else if arbitrage_chain.starting_currency == "BTC".to_owned() {
                current_balance = STARTING_BALANCE_BTC;
            } else {
                panic!("arb executor: unexpected first chain node");
            }

            let mut arb_executor_state = ArbExecutorState::Collecting;

            loop {
                tokio::select! {
                    Ok(market_update) = market_instrument_channel_receiver_0.recv() => {
                        if arbitrage_chain.is_buys[0] {
                            arbitrage_chain.orders[0].price = Some(market_update.price_ask_best);
                        } else {
                            arbitrage_chain.orders[0].price = Some(market_update.price_bid_best);
                        }
                    },
                    Ok(market_update) = market_instrument_channel_receiver_1.recv() => {
                        if arbitrage_chain.is_buys[1] {
                            arbitrage_chain.orders[1].price = Some(market_update.price_ask_best);
                        } else {
                            arbitrage_chain.orders[1].price = Some(market_update.price_bid_best);
                        }
                    },
                    Ok(market_update) = market_instrument_channel_receiver_2.recv() => {
                        if arbitrage_chain.is_buys[2] {
                            arbitrage_chain.orders[2].price = Some(market_update.price_ask_best);
                        } else {
                            arbitrage_chain.orders[2].price = Some(market_update.price_bid_best);
                        }
                    }
                }

                arb_executor_state = match arb_executor_state {
                    ArbExecutorState::Pending(timestamp) => {
                        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                        if now - timestamp > ARB_EXECUTOR_PENDING_TIMEOUT {
                            ArbExecutorState::Collecting
                        } else {
                            ArbExecutorState::Pending(timestamp)
                        }
                    }
                    ArbExecutorState::Collecting => {
                        if arbitrage_chain.orders.iter().all(|o| o.price.is_some()) {
                            ArbExecutorState::CalculationReady
                        } else {
                            ArbExecutorState::Collecting
                        }
                    }
                    ArbExecutorState::CalculationReady => {
                        let mut gain = dec!(1.0);
                        let mut outcome = current_balance;
                        for idx in 0..3 {
                            if arbitrage_chain.is_buys[idx] {
                                outcome = ((outcome * TRADING_FEE) / arbitrage_chain.orders[idx].price.unwrap())
                                    .round_dp_with_strategy(arbitrage_chain.quantity_precisions[idx] as u32, RoundingStrategy::ToZero);
                                arbitrage_chain.orders[idx].quantity = Some(outcome);
                                gain = (gain * TRADING_FEE) / arbitrage_chain.orders[idx].price.unwrap();
                            } else {
                                outcome = (outcome * TRADING_FEE)
                                    .round_dp_with_strategy(arbitrage_chain.quantity_precisions[idx] as u32, RoundingStrategy::ToZero);
                                arbitrage_chain.orders[idx].quantity = Some(outcome);
                                gain = gain * TRADING_FEE * arbitrage_chain.orders[idx].price.unwrap();
                                outcome = outcome * arbitrage_chain.orders[idx].price.unwrap();
                            }
                        }

                        if gain >= GAIN_THRESHOLD {
                            if RESEARCH_MODE {
                                let now = OffsetDateTime::from(SystemTime::now()).format(&time_format).unwrap();
                                println!("time: {}, arbitrage chain: {:#?}, gain: {}", now, arbitrage_chain, gain);
                                ArbExecutorState::CalculationReady
                            } else {
                                if let Ok(permit) = arb_chain_execution_semaphore_instance.try_acquire() {
                                    let now = OffsetDateTime::from(SystemTime::now()).format(&time_format).unwrap();
                                    println!("time: {}, arbitrage chain: {:#?}, gain: {}", now, arbitrage_chain, gain);
                                    ArbExecutorState::ExecutionReady(0, permit, false)
                                } else {
                                    ArbExecutorState::CalculationReady
                                }
                            }
                        } else {
                            ArbExecutorState::CalculationReady
                        }
                    }
                    ArbExecutorState::ExecutionReady(step, permit, false) => {
                        let id = rand::random::<u16>() as i64;
                        let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                        let request_create_order = RequestWebSocket::CreateOrder {
                            id: id,
                            nonce: nonce,
                            params: arbitrage_chain.orders[step as usize].clone(),
                        };
                        println!("ordering: {:#?}", request_create_order);
                        user_mpsc_request_sender.send(request_create_order).await.unwrap();
                        ArbExecutorState::ExecutionPending(step, permit, false, nonce)
                    }
                    ArbExecutorState::ExecutionReady(step, permit, true) => {
                        let id = rand::random::<u16>() as i64;
                        let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                        let market_order = arbitrage_chain.orders[step as usize].clone();
                        let request_cancellation_order = RequestWebSocket::CancelAllOrders {
                            id: id,
                            nonce: nonce,
                            params: OrderCancellation {
                                instrument_name: market_order.instrument_name,
                            },
                        };
                        println!("cancelling: {:#?}", request_cancellation_order);
                        user_mpsc_request_sender.send(request_cancellation_order).await.unwrap();
                        ArbExecutorState::ExecutionPending(step, permit, true, nonce)
                    }
                    ArbExecutorState::ExecutionStop(permit, needs_wait) => {
                        drop(permit);
                        let res: Option<ArbExecutorState>;
                        if needs_wait {
                            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                            res = Some(ArbExecutorState::Pending(now));
                        } else {
                            res = Some(ArbExecutorState::Collecting);
                        }
                        if let Some(state) = res {
                            state
                        } else {
                            panic!("arb executor: no state generated as a result of ExecutionStop processing");
                        }
                    }
                    ArbExecutorState::ExecutionPending(step, permit, is_cancellation, nonce) => {
                        let res: Option<ArbExecutorState>;
                        if let Ok(user_channel_msg) = user_broadcast_response_receiver.try_recv() {
                            match user_channel_msg {
                                ResponseWebSocket::CreateOrder { id: _, code, result: _ } => {
                                    if code != 0 {
                                        println!("arb executor: received non-zero code on order confirmation: {}", code);
                                        res = Some(ArbExecutorState::ExecutionStop(permit, true));
                                    } else {
                                        println!("arb executor: received order confirmation");
                                        res = Some(ArbExecutorState::ExecutionPending(step, permit, is_cancellation, nonce));
                                    }
                                }
                                ResponseWebSocket::CancelAllOrders { id: _, code } => {
                                    if code != 0 {
                                        println!(
                                            "arb executor: received non-zero code on order confirmation, breaking order execution: {}",
                                            code
                                        );
                                        res = Some(ArbExecutorState::ExecutionStop(permit, true));
                                    } else {
                                        println!("arb executor: received order confirmation");
                                        res = Some(ArbExecutorState::ExecutionPending(step, permit, is_cancellation, nonce));
                                    }
                                }
                                ResponseWebSocket::Subscribe {
                                    id: _,
                                    code: _,
                                    result: Some(ResponseResult::UserOrder { data, .. }),
                                } => {
                                    println!("arb executor: received main order response: {:#?}", data);
                                    if data[0].instrument_name == arbitrage_chain.orders[step as usize].instrument_name {
                                        if data[0].status == "ACTIVE".to_owned() {
                                            println!("arb executor: got ACTIVE status for limit order, waiting");
                                            res = Some(ArbExecutorState::ExecutionPending(step, permit, is_cancellation, nonce));
                                        } else if data[0].status == "REJECTED".to_owned() {
                                            println!("arb executor: got REJECTED status for limit order, breaking order execution");
                                            res = Some(ArbExecutorState::ExecutionStop(permit, true));
                                        } else if data[0].status == "CANCELED".to_owned() && !is_cancellation {
                                            println!("arb executor: got CANCELED status for LIMIT order, breaking order execution");
                                            res = Some(ArbExecutorState::ExecutionStop(permit, false));
                                        } else if data[0].status == "CANCELED".to_owned() && is_cancellation {
                                            println!("arb executor: got CANCELED status for CANCELED order, successfully breaking order execution");
                                            res = Some(ArbExecutorState::ExecutionStop(permit, true));
                                        } else if data[0].status == "FILLED".to_owned() && !is_cancellation {
                                            if step == 0 || step == 1 {
                                                if arbitrage_chain.is_buys[step as usize] && !arbitrage_chain.is_buys[(step + 1) as usize] {
                                                    arbitrage_chain.orders[(step + 1) as usize].quantity =
                                                        Some((data[0].cumulative_quantity * TRADING_FEE).round_dp_with_strategy(
                                                            arbitrage_chain.quantity_precisions[(step + 1) as usize] as u32,
                                                            RoundingStrategy::ToZero,
                                                        ));
                                                } else if arbitrage_chain.is_buys[step as usize] && arbitrage_chain.is_buys[(step + 1) as usize] {
                                                    arbitrage_chain.orders[(step + 1) as usize].quantity = Some(
                                                        ((data[0].cumulative_quantity * TRADING_FEE)
                                                            / arbitrage_chain.orders[(step + 1) as usize].price.unwrap())
                                                        .round_dp_with_strategy(
                                                            arbitrage_chain.quantity_precisions[(step + 1) as usize] as u32,
                                                            RoundingStrategy::ToZero,
                                                        ),
                                                    );
                                                } else if !arbitrage_chain.is_buys[step as usize] && arbitrage_chain.is_buys[(step + 1) as usize] {
                                                    arbitrage_chain.orders[(step + 1) as usize].quantity = Some(
                                                        ((data[0].cumulative_value * TRADING_FEE)
                                                            / arbitrage_chain.orders[(step + 1) as usize].price.unwrap())
                                                        .round_dp_with_strategy(
                                                            arbitrage_chain.quantity_precisions[(step + 1) as usize] as u32,
                                                            RoundingStrategy::ToZero,
                                                        ),
                                                    );
                                                } else if !arbitrage_chain.is_buys[step as usize] && !arbitrage_chain.is_buys[(step + 1) as usize] {
                                                    arbitrage_chain.orders[(step + 1) as usize].quantity =
                                                        Some((data[0].cumulative_value * TRADING_FEE).round_dp_with_strategy(
                                                            arbitrage_chain.quantity_precisions[(step + 1) as usize] as u32,
                                                            RoundingStrategy::ToZero,
                                                        ));
                                                } else {
                                                    panic!("arb executor: bad is_buys combination");
                                                }
                                                println!("arb executor: order filled, proceeding execution");
                                                res = Some(ArbExecutorState::ExecutionReady(step + 1, permit, false));
                                            } else if step == 2 {
                                                println!("arb executor: successfully finished, breaking order execution");
                                                res = Some(ArbExecutorState::ExecutionStop(permit, false));
                                            } else {
                                                panic!("arb executor: bad step value");
                                            }
                                        } else {
                                            panic!("arb executor: bad order status scenario");
                                        }
                                    } else {
                                        res = Some(ArbExecutorState::ExecutionPending(step, permit, is_cancellation, nonce));
                                    }
                                }
                                _ => {
                                    panic!("arb executor: received unknown message");
                                }
                            }
                        } else {
                            let nonce_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                            if nonce_now - nonce > ARB_EXECUTOR_ORDER_TIMEOUT {
                                println!("arb executor: having ACTIVE status for limit order for too long, cancelling");
                                res = Some(ArbExecutorState::ExecutionReady(step, permit, true));
                            } else {
                                res = Some(ArbExecutorState::ExecutionPending(step, permit, is_cancellation, nonce));
                            }
                        };
                        if let Some(state) = res {
                            state
                        } else {
                            panic!("arb executor: no state generated as a result of ExecutionPending processing");
                        }
                    }
                };
            }
        });
        tasks.push(handle);
    }

    println!("main: spawned the tasks");
    println!("main: awaiting on the websockets tasks handles");
    market_ws.request_task_handle.await.unwrap();
}
