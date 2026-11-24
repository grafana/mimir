use std::any::Any;
use std::marker::PhantomData;

use prost::Message;
use prost::bytes::BufMut;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, EncodeBuf, Encoder};

const BUFFER_SIZE: usize = 128 * 1024;
const YIELD_THRESHOLD: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub struct FastProstCodec<T, U> {
    marker: PhantomData<(T, U)>,
}

impl<T, U> Default for FastProstCodec<T, U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T, U> Codec for FastProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = FastProstEncoder<T>;
    type Decoder = tonic_prost::ProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        FastProstEncoder {
            marker: PhantomData,
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        tonic_prost::ProstCodec::<T, U>::raw_decoder(BufferSettings::default())
    }
}

pub struct FastProstEncoder<T> {
    marker: PhantomData<T>,
}

impl<T> Encoder for FastProstEncoder<T>
where
    T: Message + Send + 'static,
{
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buffer: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        if let Some(response) =
            (&item as &dyn Any).downcast_ref::<crate::proto::cortex::QueryStreamResponse>()
            && !response.encoded_response.is_empty()
        {
            buffer.put_slice(&response.encoded_response);
            return Ok(());
        }
        item.encode(buffer)
            .expect("message only fails when the buffer has insufficient capacity");
        Ok(())
    }

    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(BUFFER_SIZE, YIELD_THRESHOLD)
    }
}
