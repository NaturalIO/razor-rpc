/// A trait to adapt various type of buffer
pub trait AllocateBuf: 'static + Sized + Send {
    /// Alloc buffer or reserve space to fit blob_len inside the Buffer.
    ///
    /// When size is not enough, return None
    fn reserve(&mut self, _blob_len: u32) -> Option<&mut [u8]>;
}

/// If Option is None, create a new `Vec<u8>` on call, otherwise grow to fit the requirement
impl AllocateBuf for Option<Vec<u8>> {
    #[inline]
    #[allow(clippy::uninit_vec)]
    fn reserve(&mut self, blob_len: u32) -> Option<&mut [u8]> {
        let blob_len = blob_len as usize;
        if let Some(buf) = self.as_mut() {
            if buf.len() != blob_len {
                if buf.capacity() >= blob_len {
                } else {
                    buf.reserve(blob_len - buf.capacity());
                }
                unsafe { buf.set_len(blob_len) };
            }
        } else {
            let mut v = Vec::with_capacity(blob_len);
            unsafe { v.set_len(blob_len) };
            self.replace(v);
        }
        return self.as_deref_mut();
    }
}

/// Grow to fit the requirement
impl AllocateBuf for Vec<u8> {
    #[inline]
    fn reserve(&mut self, blob_len: u32) -> Option<&mut [u8]> {
        let blob_len = blob_len as usize;
        if self.len() != blob_len {
            if self.capacity() >= blob_len {
            } else {
                self.reserve(blob_len - self.capacity());
            }
            unsafe { self.set_len(blob_len) };
        }
        return Some(self);
    }
}

/// If Option is None, create a new [io_buffer::Buffer](https://docs.rs/io_buffer) on call.
/// Otherwise will check the pre-allocated buffer.
///
/// RPC will return encode error or decode error when the size is not enough.
///
/// # Safety
///
/// io-buffer::Buffer only supports i32::MAX, otherwise will panic
impl AllocateBuf for Option<io_buffer::Buffer> {
    #[inline]
    fn reserve(&mut self, blob_len: u32) -> Option<&mut [u8]> {
        if let Some(buf) = self.as_mut() {
            let blob_len = blob_len as usize;
            if buf.len() != blob_len {
                if buf.capacity() < blob_len {
                    return None;
                }
                buf.set_len(blob_len);
            }
        } else {
            assert!(blob_len < i32::MAX as u32, "message too large for Buffer");
            if let Ok(v) = io_buffer::Buffer::alloc(blob_len as i32) {
                self.replace(v);
            } else {
                error!("alloc failed for blob_len: {blob_len}");
                // alloc failed
                return None;
            }
        }
        return self.as_deref_mut();
    }
}

/// Check an pre-allocated [io_buffer::Buffer](https://docs.rs/io_buffer).
///
/// RPC will return encode error or decode error when the size is not enough.
impl AllocateBuf for io_buffer::Buffer {
    #[inline]
    fn reserve(&mut self, blob_len: u32) -> Option<&mut [u8]> {
        let blob_len = blob_len as usize;
        if self.len() == blob_len {
        } else {
            if self.capacity() < blob_len {
                return None;
            }
            self.set_len(blob_len);
        }
        Some(self)
    }
}

#[cfg(feature = "smallvec")]
impl<A: smallvec::Array<Item = u8> + Send + 'static> AllocateBuf for smallvec::SmallVec<A>
where
    A::Item: Send,
{
    #[inline]
    fn reserve(&mut self, blob_len: u32) -> Option<&mut [u8]> {
        let blob_len = blob_len as usize;
        if self.len() != blob_len {
            if self.capacity() >= blob_len {
            } else {
                self.reserve(blob_len - self.capacity());
            }
            unsafe { self.set_len(blob_len) };
        }
        return Some(self.as_mut());
    }
}
