use crate::{
    Codec,
    decode2::{Fsm, FsmControlFlow, FsmResult},
};
use oval::Buffer;

pub struct CodecStateMachine<T: Fsm> {
    sub_machine: Option<T>,
    codec: Decoder,
    buffer: Buffer,
}

impl<T: Fsm> CodecStateMachine<T> {
    pub fn new(sub_machine: T, codec: Codec) -> Self {
        Self {
            sub_machine: Some(sub_machine),
            codec: codec.into(),
            buffer: Buffer::with_capacity(1024),
        }
    }

    pub fn reset(&mut self, sub_machine: T) {
        self.buffer.reset();
        self.sub_machine = Some(sub_machine);
        self.codec.reset();
    }
}

pub enum Decoder {
    Null,
    Deflate(Box<miniz_oxide::inflate::stream::InflateState>),
    #[cfg(feature = "snappy")]
    Snappy(snap::raw::Decoder),
    #[cfg(feature = "zstandard")]
    Zstandard(zstd::stream::raw::Decoder<'static>),
    #[cfg(feature = "bzip")]
    Bzip2(bzip2::Decompress),
    #[cfg(feature = "xz")]
    Xz(liblzma::stream::Stream),
}

impl From<Codec> for Decoder {
    fn from(value: Codec) -> Self {
        match value {
            Codec::Null => Self::Null,
            Codec::Deflate(_) => {
                use miniz_oxide::{DataFormat::Raw, inflate::stream::InflateState};
                Self::Deflate(InflateState::new_boxed(Raw))
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => Self::Snappy(snap::raw::Decoder::new()),
            #[cfg(feature = "zstandard")]
            Codec::Zstandard(_) => Self::Zstandard(zstd::stream::raw::Decoder::new().unwrap()),
            #[cfg(feature = "bzip")]
            Codec::Bzip2(_) => Self::Bzip2(bzip2::Decompress::new(false)),
            #[cfg(feature = "xz")]
            Codec::Xz(_) => {
                Self::Xz(liblzma::stream::Stream::new_auto_decoder(u64::MAX, 0).unwrap())
            }
        }
    }
}

impl Decoder {
    pub fn reset(&mut self) {
        match self {
            Decoder::Null => {}
            Decoder::Deflate(decoder) => {
                decoder.reset_as(miniz_oxide::inflate::stream::MinReset);
            }
            #[cfg(feature = "snappy")]
            Decoder::Snappy(_decoder) => {} // No reset needed
            #[cfg(feature = "zstandard")]
            Decoder::Zstandard(decoder) => zstd::stream::raw::Operation::reinit(decoder).unwrap(),
            #[cfg(feature = "bzip")]
            Decoder::Bzip2(decoder) => {
                // No reset/reinit API available
                let _drop = std::mem::replace(decoder, bzip2::Decompress::new(false));
            }
            #[cfg(feature = "xz")]
            Decoder::Xz(decoder) => {
                // No reset/reinit API available
                let _drop = std::mem::replace(
                    decoder,
                    liblzma::stream::Stream::new_auto_decoder(u64::MAX, 0).unwrap(),
                );
            }
        }
    }
}

impl<T: Fsm> Fsm for CodecStateMachine<T> {
    type Output = (T::Output, Self);

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        let buffer = match &mut self.codec {
            Decoder::Null => buffer,
            Decoder::Deflate(decoder) => {
                use miniz_oxide::{MZFlush, StreamResult, inflate::stream::inflate};
                let StreamResult {
                    bytes_consumed,
                    bytes_written,
                    status,
                } = inflate(decoder, buffer.data(), self.buffer.space(), MZFlush::None);
                status.unwrap();
                buffer.consume(bytes_consumed);
                self.buffer.fill(bytes_written);

                &mut self.buffer
            }
            #[cfg(feature = "snappy")]
            Decoder::Snappy(_decoder) => {
                todo!("Snap has no streaming decoder")
            }
            #[cfg(feature = "zstandard")]
            Decoder::Zstandard(decoder) => {
                use zstd::stream::raw::{Operation, Status};
                let Status {
                    bytes_read,
                    bytes_written,
                    ..
                } = decoder
                    .run_on_buffers(buffer.data(), self.buffer.space())
                    .map_err(crate::error::Details::ZstdDecompress)?;
                buffer.consume(bytes_read);
                self.buffer.fill(bytes_written);

                &mut self.buffer
            }
            #[cfg(feature = "bzip")]
            Decoder::Bzip2(decoder) => {
                let prev_total_in = decoder.total_in();
                let prev_total_out = decoder.total_out();

                let _status = decoder
                    .decompress(buffer.data(), self.buffer.space())
                    .unwrap();

                let consumed = decoder.total_in() - prev_total_in;
                let filled = decoder.total_out() - prev_total_out;

                buffer.consume(usize::try_from(consumed).unwrap());
                self.buffer.fill(usize::try_from(filled).unwrap());

                &mut self.buffer
            }
            #[cfg(feature = "xz")]
            Decoder::Xz(decoder) => {
                use liblzma::stream::Action::Run;

                let prev_total_in = decoder.total_in();
                let prev_total_out = decoder.total_out();

                let _status = decoder
                    .process(buffer.data(), self.buffer.space(), Run)
                    .unwrap();

                let consumed = decoder.total_in() - prev_total_in;
                let filled = decoder.total_out() - prev_total_out;

                buffer.consume(usize::try_from(consumed).unwrap());
                self.buffer.fill(usize::try_from(filled).unwrap());

                &mut self.buffer
            }
        };
        match self
            .sub_machine
            .take()
            .expect("CodecStateMachine was not reset!")
            .parse(buffer)?
        {
            FsmControlFlow::NeedMore(fsm) => {
                self.sub_machine = Some(fsm);
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done(result) => {
                Ok(FsmControlFlow::Done((result, self)))
            }
        }
    }
}
