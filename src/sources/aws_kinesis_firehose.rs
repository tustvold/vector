use crate::{
    config::{log_schema, DataType, GlobalOptions, SourceConfig},
    event::Event,
    internal_events::{
        AwsKinesisFirehoseRequestParseError, AwsKinesisFirehoseRequestReadError,
        AwsKinesisFirehoseRequestReceived,
    },
    shutdown::ShutdownSignal,
    sources::util::{ErrorMessage, HttpSource},
    tls::TlsConfig,
    Pipeline,
};
use async_trait::async_trait;
use bytes::{buf::BufExt, Bytes};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader, Read},
    net::SocketAddr,
    str::FromStr,
};
use warp::http::{HeaderMap, StatusCode};

// TODO:
// * Try to refactor reading encoded records to stream rather than copy into buffers

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AwsKinesisFirehoseConfig {
    address: SocketAddr,
    access_key: Option<String>,
    encoding: EncodingConfig<Encoding>,
    tls: Option<TlsConfig>,
}

// TODO move this to shared config
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
struct EncodingConfig<E> {
    codec: E,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
enum Encoding {
    Text,
    Json,
    AwsCloudWatchLogsSubscription,
}

#[derive(Clone, Default)]
struct AwsKinesisFirehoseSource {}

impl HttpSource for AwsKinesisFirehoseSource {
    fn build_event(&self, body: Bytes, header_map: HeaderMap) -> Result<Vec<Event>, ErrorMessage> {
        match get_header(&header_map, "X-Amz-Firehose-Protocol-Version")? {
            Some("1.0") => decode_v1_message(body, header_map),
            None => {
                return Err(header_error_message(
                    "X-Amz-Firehose-Protocol-Version",
                    "Did not find expected X-Amz-Firehose-Protocol-Version header",
                ));
            }
            Some(version) => {
                return Err(header_error_message(
                    "X-Amz-Firehose-Protocol-Version",
                    &format!("Only Firehose Protocol 1.0 supported, found: {}", version),
                ));
            }
        }
    }
}

#[typetag::serde(name = "aws_kinesis_firehose")]
#[async_trait]
impl SourceConfig for AwsKinesisFirehoseConfig {
    fn build(
        &self,
        _name: &str,
        _globals: &GlobalOptions,
        _shutdown: ShutdownSignal,
        _out: Pipeline,
    ) -> crate::Result<super::Source> {
        unimplemented!()
    }

    async fn build_async(
        &self,
        _: &str,
        _: &GlobalOptions,
        shutdown: ShutdownSignal,
        out: Pipeline,
    ) -> crate::Result<super::Source> {
        let source = AwsKinesisFirehoseSource::default();
        source.run(self.address, "events", &self.tls, out, shutdown)
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn source_type(&self) -> &'static str {
        "aws_kinesis_firehose"
    }
}

//fn validate_protocol_version(version: Option<&str>) -> Result<(), ErrorMessage> {
//match version {
//Some("1.0") => Ok(()),
//None => {
//return Err(header_error_message(
//"X-Amz-Firehose-Protocol-Version",
//"Did not find expected X-Amz-Firehose-Protocol-Version header",
//));
//}
//version => {
//return Err(header_error_message(
//"X-Amz-Firehose-Protocol-Version",
//&format!("Only Firehose Protocol 1.0 supported, found: {}", version),
//));
//}
//}
//}

fn decode_v1_message(body: Bytes, header_map: HeaderMap) -> Result<Vec<Event>, ErrorMessage> {
    //let request_id = get_header(&header_map, "X-Amz-Firehose-Request-Id")?;
    //let source_arn = get_header(&header_map, "X-Amz-Firehose-Source-Arn")?;
    //let access_key = get_header(&header_map, "X-Amz-Firehose-Access-Key")?;
    let request: FirehoseRequest = serde_json::from_reader(body.reader()).map_err(|error| {
        //TODO
        //emit!(AwsKinesisFirehoseRequestParseError {
        //error: Box::new(error),
        //});
        ErrorMessage::new(
            StatusCode::BAD_REQUEST,
            format!("Could not parse body as JSON: {}", error),
        )
    })?;

    // TODO avoid intermediate collections

    let records: Vec<Vec<u8>> = request
        .records
        .iter()
        .enumerate()
        .map(|(i, record)| {
            decode_record(record).map_err(|error| {
                //TODO
                //emit!(AwsKinesisFirehoseRequestParseError {
                //error: Box::new(error),
                //});
                ErrorMessage::new(
                    StatusCode::BAD_REQUEST,
                    format!("Could not decode record with index {}: {}", i, error),
                )
            })
        })
        .collect::<Result<Vec<Vec<u8>>, ErrorMessage>>()?;

    let records: Vec<Event> = records
        .iter()
        .enumerate()
        .map(|(i, record)| {
            parse_record(record).map_err(|error| {
                //TODO
                //emit!(AwsKinesisFirehoseRequestParseError {
                //error: Box::new(error),
                //});
                ErrorMessage::new(
                    StatusCode::BAD_REQUEST,
                    format!("Could not parse record with index {}: {}", i, error),
                )
            })
        })
        .collect::<Result<Vec<Vec<Event>>, ErrorMessage>>()?
        .into_iter()
        .flatten()
        .collect();

    dbg!(&records);
    Ok(records)
}

// TODO handle other codecs
fn parse_record(record: &Vec<u8>) -> Result<Vec<Event>, serde_json::error::Error> {
    let record: AwsCloudWatchLogsSubscriptionRecord = serde_json::from_reader(record.reader())?;
    //.map_err(|error| {
    //emit!(AwsKinesisFirehoseRequestParseError {
    //error: error.into()
    //});
    //ErrorMessage::new(
    //StatusCode::BAD_REQUEST,
    //format!("Could not parse body as JSON: {}", error),
    //)
    //})?;

    Ok(match record {
        AwsCloudWatchLogsSubscriptionRecord::ControlMessage(_) => vec![], // TODO?
        AwsCloudWatchLogsSubscriptionRecord::DataMessage(message) => message
            .log_events
            .into_iter()
            .map(|log_event| {
                let mut event = Event::from(log_event.message.as_str());
                let log = event.as_mut_log();
                log.insert(
                    log_schema().timestamp_key().clone(),
                    log_event.timestamp.clone(),
                );
                event
            })
            .collect(),
    })
}

#[derive(Debug, Deserialize)]
#[serde(tag = "messageType")]
enum AwsCloudWatchLogsSubscriptionRecord {
    #[serde(rename = "DATA_MESSAGE")]
    DataMessage(AwsCloudWatchLogsSubscriptionDataMessage),
    #[serde(rename = "CONTROL_MESSAGE")]
    ControlMessage(AwsCloudWatchLogsSubscriptionControlMessage),
}

// TODO
#[derive(Debug, Deserialize)]
struct AwsCloudWatchLogsSubscriptionControlMessage {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AwsCloudWatchLogsSubscriptionDataMessage {
    owner: u32,
    log_group: String,
    log_stream: String,
    subscription_filters: Vec<String>,
    log_events: Vec<AwsCloudWatchLogEvent>,
}

#[derive(Debug, Deserialize)]
struct AwsCloudWatchLogEvent {
    id: String,
    #[serde(with = "ts_milliseconds")]
    timestamp: DateTime<Utc>,
    message: String,
}

//{
//"messageType": "DATA_MESSAGE",
//"owner": "071959437513",
//"logGroup": "/jesse/test",
//"logStream": "test",
//"subscriptionFilters": [
//"Destination"
//],
//"logEvents": [
//{
//"id": "35683658089614582423604394983260738922885519999578275840",
//"timestamp": 1600110569039,
//"message": "{\"bytes\":26780,\"datetime\":\"14/Sep/2020:11:45:41 -0400\",\"host\":\"157.130.216.193\",\"method\":\"PUT\",\"protocol\":\"HTTP/1.0\",\"referer\":\"https://www.principalcross-platform.io/markets/ubiquitous\",\"request\":\"/expedite/convergence\",\"source_type\":\"stdin\",\"status\":301,\"user-identifier\":\"-\"}"
//},

//fn handle_request(body: Bytes, header_map: HeaderMap) -> Result<Vec<Event>, ErrorMessage> {
//let protocol_version = get_header(&header_map, "X-Amz-Firehose-Protocol-Version")?;

//match protocol_version {
//Some("1.0") => decode_v1_message(body, header_map),
//None => {
//return Err(header_error_message(
//"X-Amz-Firehose-Protocol-Version",
//"Did not find expected X-Amz-Firehose-Protocol-Version header",
//));
//}
//Some(version) => {
//return Err(header_error_message(
//"X-Amz-Firehose-Protocol-Version",
//&format!("Only Firehose Protocol 1.0 supported, found: {}", version),
//));
//}
//}
//.map_err(|error| emit!(AwsKinesisFirehoseRequestParseError { error }))?;

////emit!(AwsKinesisFirehoseRequestReceived {
////msg_count,
////frame_id,
////drain_token
////});

////// Deal with body
////let events = body_to_events(body);

////if events.len() != msg_count {
////let error_msg = format!(
////"Parsed event count does not match message count header: {} vs {}",
////events.len(),
////msg_count
////);

////if cfg!(test) {
////panic!(error_msg);
////}
////return Err(header_error_message(
////"AwsKinesisFirehose-Msg-Count",
////&error_msg,
////));
////}

////Ok(events)
//}

/// return the parsed header, if it exists
fn get_header<'a>(header_map: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ErrorMessage> {
    header_map
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(|value| Some(value))
                .map_err(|e| header_error_message(name, &e.to_string()))
        })
        .unwrap_or(Ok(None))
}

fn header_error_message(name: &str, msg: &str) -> ErrorMessage {
    ErrorMessage::new(
        StatusCode::BAD_REQUEST,
        format!("Invalid request header {:?}: {:?}", name, msg),
    )
}

/// decode record from its base64 gzip format
fn decode_record(record: &EncodedFirehoseRecord) -> std::io::Result<Vec<u8>> {
    use flate2::read::GzDecoder;

    let mut cursor = std::io::Cursor::new(record.data.as_bytes());
    let base64decoder = base64::read::DecoderReader::new(&mut cursor, base64::STANDARD);

    let mut gz = GzDecoder::new(base64decoder);
    let mut buffer = Vec::new();
    gz.read_to_end(&mut buffer)?;

    Ok(buffer)
}

fn line_to_event(line: String) -> Event {
    let parts = line.splitn(8, ' ').collect::<Vec<&str>>();

    let mut event = if parts.len() == 8 {
        let timestamp = parts[2];
        let hostname = parts[3];
        let app_name = parts[4];
        let proc_id = parts[5];
        let message = parts[7];

        let mut event = Event::from(message);
        let log = event.as_mut_log();

        if let Ok(ts) = timestamp.parse::<DateTime<Utc>>() {
            log.insert(log_schema().timestamp_key().clone(), ts);
        }

        log.insert(log_schema().host_key().clone(), hostname.to_owned());

        log.insert("app_name", app_name.to_owned());
        log.insert("proc_id", proc_id.to_owned());

        event
    } else {
        warn!(
            message = "Line didn't match expected logplex format, so raw message is forwarded.",
            fields = parts.len(),
            rate_limit_secs = 10
        );
        Event::from(line)
    };

    // Add source type
    event.as_mut_log().try_insert(
        log_schema().source_type_key(),
        Bytes::from("aws_kinesis_firehose"),
    );

    event
}

#[derive(Deserialize)]
struct FirehoseRequest {
    #[serde(rename = "requestId")]
    request_id: String,

    #[serde(rename = "timestamp")]
    timestamp: String,

    records: Vec<EncodedFirehoseRecord>,
}

#[derive(Deserialize)]
struct EncodedFirehoseRecord {
    data: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown::ShutdownSignal;
    use crate::{
        config::{log_schema, GlobalOptions, SourceConfig},
        event::Event,
        test_util::{collect_n, next_addr, trace_init, wait_for_tcp},
        Pipeline,
    };
    use base64;
    use chrono::{DateTime, Utc};
    use futures::compat::Future01CompatExt;
    use futures01::sync::mpsc;
    use pretty_assertions::assert_eq;
    use std::io::Read;
    use std::net::SocketAddr;

    async fn source(
        access_key: Option<String>,
        encoding: EncodingConfig<Encoding>,
    ) -> (mpsc::Receiver<Event>, SocketAddr) {
        let (sender, recv) = Pipeline::new_test();
        let address = next_addr();
        tokio::spawn(async move {
            AwsKinesisFirehoseConfig {
                address,
                tls: None,
                access_key,
                encoding,
            }
            .build_async(
                "default",
                &GlobalOptions::default(),
                ShutdownSignal::noop(),
                sender,
            )
            .await
            .unwrap()
            .compat()
            .await
            .unwrap()
        });
        wait_for_tcp(address).await;
        (recv, address)
    }

    /// Sends the body to the address with the appropriate Firehose headers
    ///
    /// https://docs.aws.amazon.com/firehose/latest/dev/httpdeliveryrequestresponse.html
    async fn send(address: SocketAddr, body: &str, key: &str) -> u16 {
        reqwest::Client::new()
            .post(&format!("http://{}", address))
            .header("host", address.to_string())
            .header(
                "x-amzn-trace-id",
                "Root=1-5f5fbf1c-877c68cace58bea222ddbeec",
            )
            .header("content-length", body.len())
            .header("x-amz-firehose-protocol-version", "1.0")
            .header(
                "x-amz-firehose-request-id",
                "e17265d6-97af-4938-982e-90d5614c4242",
            )
            .header(
                "x-amz-firehose-source-arn",
                "arn:aws:firehose:us-east-1:111111111111:deliverystream/test",
            )
            .header("x-amz-firehose-access-key", key)
            .header("user-agent", "Amazon Kinesis Data Firehose Agent/1.0")
            .header("content-type", "application/json")
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    /// Encodes record data to mach AWS's representation: base64 encoded, gzip'd data
    fn encode_record(record: &str) -> std::io::Result<String> {
        use flate2::read::GzEncoder;
        use flate2::Compression;

        let mut buffer = Vec::new();

        let mut gz = GzEncoder::new(record.as_bytes(), Compression::fast());
        gz.read(&mut buffer)?;

        Ok(base64::encode(&buffer))
    }

    #[tokio::test]
    async fn firehose_handles_cloudwatch_logs() {
        let record = r#"
{
  "messageType": "DATA_MESSAGE",
  "owner": "071959437513",
  "logGroup": "/jesse/test",
  "logStream": "test",
  "subscriptionFilters": [
	"Destination"
  ],
  "logEvents": [
	{
	  "id": "35683658089614582423604394983260738922885519999578275840",
	  "timestamp": 1600110569039,
	  "message": "{\"bytes\":26780,\"datetime\":\"14/Sep/2020:11:45:41 -0400\",\"host\":\"157.130.216.193\",\"method\":\"PUT\",\"protocol\":\"HTTP/1.0\",\"referer\":\"https://www.principalcross-platform.io/markets/ubiquitous\",\"request\":\"/expedite/convergence\",\"source_type\":\"stdin\",\"status\":301,\"user-identifier\":\"-\"}"
	},
	{
	  "id": "35683658089659183914001456229543810359430816722590236673",
	  "timestamp": 1600110569041,
	  "message": "{\"bytes\":17707,\"datetime\":\"14/Sep/2020:11:45:41 -0400\",\"host\":\"109.81.244.252\",\"method\":\"GET\",\"protocol\":\"HTTP/2.0\",\"referer\":\"http://www.investormission-critical.io/24/7/vortals\",\"request\":\"/scale/functionalities/optimize\",\"source_type\":\"stdin\",\"status\":502,\"user-identifier\":\"feeney1708\"}"
	}
  ]
}
"#;

        let body = r#"
{
  "requestId": "e17265d6-97af-4938-982e-90d5614c4242",
  "timestamp": 1600110364268,
  "records": [
    {
      "data": "%record%"
    }
  ]
}
"#
        .replace("%record%", &encode_record(record).unwrap());

        let (rx, addr) = source(
            None,
            EncodingConfig {
                codec: Encoding::AwsCloudWatchLogsSubscription,
            },
        )
        .await;

        assert_eq!(200, send(addr, &body, "").await);

        let events = collect_n(rx, 2).await.unwrap();
        //let event = events.remove(0);
        //let log = event.as_log();

        assert_eq!(events, vec![]);

        //assert_eq!(
        //log[&log_schema().message_key()],
        //r#"at=info method=GET path="/cart_link" host=lumberjack-store.timber.io request_id=05726858-c44e-4f94-9a20-37df73be9006 fwd="73.75.38.87" dyno=web.1 connect=1ms service=22ms status=304 bytes=656 protocol=http"#.into()
        //);
        //assert_eq!(
        //log[&log_schema().timestamp_key()],
        //"2020-01-08T22:33:57.353034+00:00"
        //.parse::<DateTime<Utc>>()
        //.unwrap()
        //.into()
        //);
        //assert_eq!(log[&log_schema().host_key()], "host".into());
        //assert_eq!(log[log_schema().source_type_key()], "logplex".into());
    }

    //#[test]
    //fn logplex_handles_normal_lines() {
    //let body = "267 <158>1 2020-01-08T22:33:57.353034+00:00 host heroku router - foo bar baz";
    //let event = super::line_to_event(body.into());
    //let log = event.as_log();

    //assert_eq!(log[&log_schema().message_key()], "foo bar baz".into());
    //assert_eq!(
    //log[&log_schema().timestamp_key()],
    //"2020-01-08T22:33:57.353034+00:00"
    //.parse::<DateTime<Utc>>()
    //.unwrap()
    //.into()
    //);
    //assert_eq!(log[&log_schema().host_key()], "host".into());
    //assert_eq!(log[log_schema().source_type_key()], "logplex".into());
    //}

    //#[test]
    //fn logplex_handles_malformed_lines() {
    //let body = "what am i doing here";
    //let event = super::line_to_event(body.into());
    //let log = event.as_log();

    //assert_eq!(
    //log[&log_schema().message_key()],
    //"what am i doing here".into()
    //);
    //assert!(log.get(&log_schema().timestamp_key()).is_some());
    //assert_eq!(log[log_schema().source_type_key()], "logplex".into());
    //}

    //#[test]
    //fn logplex_doesnt_blow_up_on_bad_framing() {
    //let body = "1000000 <158>1 2020-01-08T22:33:57.353034+00:00 host heroku router - i'm not that long";
    //let event = super::line_to_event(body.into());
    //let log = event.as_log();

    //assert_eq!(log[&log_schema().message_key()], "i'm not that long".into());
    //assert_eq!(
    //log[&log_schema().timestamp_key()],
    //"2020-01-08T22:33:57.353034+00:00"
    //.parse::<DateTime<Utc>>()
    //.unwrap()
    //.into()
    //);
    //assert_eq!(log[&log_schema().host_key()], "host".into());
    //assert_eq!(log[log_schema().source_type_key()], "logplex".into());
    //}
}
