use std::io::{StdoutLock, Write};

use eyre::Context;
use serde::{Deserialize, Serialize};
use serde_json::ser::PrettyFormatter;

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
    Echo { echo: String },
    EchoOk { echo: String },
    Init { node_id: String, node_ids: Vec<String>},
    InitOk
}

struct EchoNode {
    id: usize,
}

impl EchoNode {
    pub fn step(
        &mut self,
        input: Msg,
        output: &mut StdoutLock,
    ) -> eyre::Result<()> {
        match input.body.payload {
            Payload::Init{ ..} => {
                let ans = Msg {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    },
                };

                serde_json::to_writer(&mut *output, &ans).context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write failed")?;

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

                serde_json::to_writer(&mut *output, &ans).context("Serialize::serialize failed init")?;
                output.write_all(b"\n").context("Write failed")?;

                self.id += 1;
            }
            Payload::EchoOk { .. } => {}
        };

        Ok(())
    }
}

fn main() -> eyre::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let msgs = serde_json::Deserializer::from_reader(stdin).into_iter::<Msg>();

    let mut state = EchoNode { id: 0};

    for msg in msgs {
        let mes = msg.context("STDIN::Could not deserialize")?;

        state.step(mes, &mut stdout).context("EchoNode::step failed")?;
    }

    Ok(())
}
