use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, StreamExt};
use http_body_util::{Full, StreamBody};
use hyper::body::{Bytes, Frame};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

type BytesFrameStream = Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, io::Error>> + Send>>;

pub enum ResponseBody {
    Empty,
    Fixed(Full<Bytes>),
    Streaming(StreamBody<BytesFrameStream>),
}

impl ResponseBody {
    pub fn empty() -> Self {
        ResponseBody::Empty
    }

    pub fn fixed(data: Vec<u8>) -> Self {
        let data = Bytes::from(data);
        ResponseBody::Fixed(Full::new(data))
    }

    pub fn streaming<R>(reader: R) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        let stream = ReaderStream::new(reader).map(|result| result.map(Frame::data));
        ResponseBody::Streaming(StreamBody::new(Box::pin(stream)))
    }
}

impl hyper::body::Body for ResponseBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            ResponseBody::Empty => Poll::Ready(None),
            ResponseBody::Fixed(body) => Pin::new(body).poll_frame(cx).map_err(io::Error::other),
            ResponseBody::Streaming(body) => Pin::new(body).poll_frame(cx),
        }
    }
}
