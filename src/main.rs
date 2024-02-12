use std::{
    collections::HashMap,
    io::{StdoutLock, Write},
};

use eyre::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "Message")]
struct Msg {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Generate,
    GenerateOk {
        id: String,
    },
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct Node {
    id: usize,
    messages: Vec<usize>,
}

impl Node {
    pub fn step(&mut self, input: Msg, output: &mut StdoutLock) -> eyre::Result<()> {
        match input.body.payload {
            Payload::Init { .. } => {
                let ans = Msg {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    },
                };

                serde_json::to_writer(&mut *output, &ans)
                    .context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write::failed")?;

                self.id += 1;
            }
            Payload::InitOk { .. } => {}
            Payload::Echo { echo } => {
                let ans = Msg {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };

                serde_json::to_writer(&mut *output, &ans)
                    .context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write::failed")?;

                self.id += 1;
            }
            Payload::Generate => {
                let id_ = ulid::Ulid::new();

                let ans = Msg {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk {
                            id: id_.to_string(),
                        },
                    },
                };

                serde_json::to_writer(&mut *output, &ans)
                    .context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write::failed")?;

                self.id += 1;
            }
            Payload::Broadcast { message } => {
                self.messages.push(message);

                let ans = Msg {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::BroadcastOk,
                    },
                };

                serde_json::to_writer(&mut *output, &ans)
                    .context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write::failed")?;

                self.id += 1;
            }
            Payload::Read => {
                let ans = Msg {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::ReadOk {
                            messages: self.messages.clone(),
                        },
                    },
                };

                serde_json::to_writer(&mut *output, &ans)
                    .context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write::failed")?;

                self.id += 1;
            }
            Payload::Topology { topology: _ } => {
                let ans = Msg {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::TopologyOk,
                    },
                };

                serde_json::to_writer(&mut *output, &ans)
                    .context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write::failed")?;

                self.id += 1;
            }
            _ => {}
        };

        Ok(())
    }
}

fn main() -> eyre::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let msgs = serde_json::Deserializer::from_reader(stdin).into_iter::<Msg>();

    let mut state = Node {
        id: 0,
        messages: Vec::new(),
    };

    for msg in msgs {
        let mes = msg.context("STDIN::Could not deserialize")?;

        state
            .step(mes, &mut stdout)
            .context("EchoNode::step failed")?;
    }

    Ok(())
}
