use super::InternalEvent;
use metrics::counter;
use std::error;

#[derive(Debug)]
pub struct AwsKinesisFirehoseRequestReceived<'a> {
    pub msg_count: usize,
    pub frame_id: &'a str,
    pub drain_token: &'a str,
}

impl<'a> InternalEvent for AwsKinesisFirehoseRequestReceived<'a> {
    fn emit_logs(&self) {
        info!(
            message = "Handling AWS Kinesis Firehose request.",
            msg_count = %self.msg_count,
            frame_id = %self.frame_id,
            drain_token = %self.drain_token,
            rate_limit_secs = 10
        );
    }

    fn emit_metrics(&self) {
        counter!("requests_received", 1,
            "component_kind" => "source",
            "component_type" => "aws_kinesis_firehose",
        );
    }
}

#[derive(Debug)]
pub struct AwsKinesisFirehoseRequestReadError {
    pub error: std::io::Error,
}

impl InternalEvent for AwsKinesisFirehoseRequestReadError {
    fn emit_logs(&self) {
        error!(
            message = "error reading request body.",
            error = ?self.error,
            rate_limit_secs = 10
        );
    }

    fn emit_metrics(&self) {
        counter!(
            "request_read_errors", 1,
            "component_kind" => "source",
            "component_type" => "aws_kinesis_firehose",
        );
    }
}

#[derive(Debug)]
pub struct AwsKinesisFirehoseRequestParseError {
    //pub error: serde_json::error::Error,
    //TODO use a enum
    pub error: Box<dyn error::Error>,
}

impl InternalEvent for AwsKinesisFirehoseRequestParseError {
    fn emit_logs(&self) {
        error!(
            message = "error reading request body.",
            error = ?self.error,
            rate_limit_secs = 10
        );
    }

    fn emit_metrics(&self) {
        counter!(
            "request_read_errors", 1,
            "component_kind" => "source",
            "component_type" => "aws_kinesis_firehose",
        );
    }
}
