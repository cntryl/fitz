use bytes::Bytes;
use http_body_util::Full;

pub type Body = Full<Bytes>;
pub type Request = hyper::Request<hyper::body::Incoming>;
pub type Response = hyper::Response<Body>;
