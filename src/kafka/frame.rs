use std::{error::Error, fmt, io};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    InvalidLength(i32),
    TooLarge(usize),
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "Kafka frame I/O failed: {error}"),
            Self::InvalidLength(length) => {
                write!(formatter, "Kafka frame length cannot be negative: {length}")
            }
            Self::TooLarge(length) => write!(
                formatter,
                "Kafka frame length {length} exceeds the {MAX_FRAME_SIZE}-byte limit"
            ),
        }
    }
}

impl Error for FrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidLength(_) | Self::TooLarge(_) => None,
        }
    }
}

impl From<io::Error> for FrameError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub async fn read_frame<R>(reader: &mut R) -> Result<Option<Bytes>, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0_u8; 4];
    let mut prefix_bytes_read = 0;

    while prefix_bytes_read < prefix.len() {
        let bytes_read = reader.read(&mut prefix[prefix_bytes_read..]).await?;
        if bytes_read == 0 {
            if prefix_bytes_read == 0 {
                return Ok(None);
            }
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
        prefix_bytes_read += bytes_read;
    }

    let length = decode_frame_length(i32::from_be_bytes(prefix))?;
    let mut body = BytesMut::zeroed(length);
    reader.read_exact(&mut body).await?;

    Ok(Some(body.freeze()))
}

pub async fn write_frame<W>(writer: &mut W, body: &[u8]) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    if body.len() > MAX_FRAME_SIZE {
        return Err(FrameError::TooLarge(body.len()));
    }

    let length = i32::try_from(body.len()).expect("maximum frame size fits in i32");
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

fn decode_frame_length(length: i32) -> Result<usize, FrameError> {
    let length = usize::try_from(length).map_err(|_| FrameError::InvalidLength(length))?;
    if length > MAX_FRAME_SIZE {
        return Err(FrameError::TooLarge(length));
    }
    Ok(length)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{FrameError, MAX_FRAME_SIZE, decode_frame_length, read_frame, write_frame};

    #[tokio::test]
    async fn reads_one_complete_frame_then_reports_clean_eof() {
        let (mut client, mut server) = tokio::io::duplex(32);
        client
            .write_all(&[0, 0, 0, 3, 1, 2, 3])
            .await
            .expect("write test frame");
        drop(client);

        assert_eq!(
            read_frame(&mut server).await.expect("read frame"),
            Some(Bytes::from_static(&[1, 2, 3]))
        );
        assert_eq!(read_frame(&mut server).await.expect("read clean EOF"), None);
    }

    #[test]
    fn rejects_negative_and_oversized_lengths_before_allocating() {
        assert!(matches!(
            decode_frame_length(-1),
            Err(FrameError::InvalidLength(-1))
        ));
        assert!(matches!(
            decode_frame_length((MAX_FRAME_SIZE + 1) as i32),
            Err(FrameError::TooLarge(size)) if size == MAX_FRAME_SIZE + 1
        ));
    }

    #[tokio::test]
    async fn rejects_truncated_prefixes_and_bodies() {
        let (mut prefix_client, mut prefix_server) = tokio::io::duplex(8);
        prefix_client
            .write_all(&[0, 0])
            .await
            .expect("write partial prefix");
        drop(prefix_client);

        let prefix_error = read_frame(&mut prefix_server)
            .await
            .expect_err("partial prefix must fail");
        assert!(matches!(
            prefix_error,
            FrameError::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof
        ));

        let (mut body_client, mut body_server) = tokio::io::duplex(8);
        body_client
            .write_all(&[0, 0, 0, 3, 1, 2])
            .await
            .expect("write partial body");
        drop(body_client);

        let body_error = read_frame(&mut body_server)
            .await
            .expect_err("partial body must fail");
        assert!(matches!(
            body_error,
            FrameError::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof
        ));
    }

    #[tokio::test]
    async fn writes_a_bounded_length_prefixed_frame() {
        let (mut client, mut server) = tokio::io::duplex(16);

        write_frame(&mut client, &[4, 5, 6])
            .await
            .expect("write frame");

        let mut encoded = [0_u8; 7];
        server
            .read_exact(&mut encoded)
            .await
            .expect("read encoded frame");
        assert_eq!(encoded, [0, 0, 0, 3, 4, 5, 6]);
    }
}
