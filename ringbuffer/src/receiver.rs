use std::{
    ptr::NonNull,
    slice,
    sync::atomic::{compiler_fence, Ordering},
};

use crate::{
    aeron_align, aeron_rb_message_offset, descriptor::ReceiverDescriptor, RecordDescriptor,
    AERON_RB_ALIGNMENT, AERON_RB_PADDING_MSG_TYPE_ID, AERON_RB_RECORD_HEADER_LENGTH,
};

// IDEA: capacity could be a const generic (<const N: usize>)
pub struct Receiver {
    pub(crate) buffer: NonNull<u8>,
    pub(crate) capacity: usize,
    pub(crate) descriptor: ReceiverDescriptor,
    pub(crate) _max_message_length: usize,
}

unsafe impl Send for Receiver {}

impl Receiver {
    // simplified version of read without handler.
    // TODO: implement full version
    // Idea: return reference to bytes and only increment once dropped
    pub fn receive(&mut self, message_count_limit: usize) -> Vec<(i32, Vec<u8>)> {
        let head: i64 = self.descriptor.head_position.load_atomic(Ordering::Relaxed);
        let head_index: usize = head as usize & (self.capacity - 1);
        let contiguous_block_length: usize = self.capacity - head_index;
        let mut messages_read: usize = 0;
        let mut bytes_read: usize = 0;

        // TODO: replace buffer by message handler
        let mut read_buffer: Vec<(i32, Vec<u8>)> = Vec::new();

        while bytes_read < contiguous_block_length && messages_read < message_count_limit {
            let record_index: usize = head_index + bytes_read;
            let header: &RecordDescriptor = {
                let ptr: *mut u8 = unsafe { self.buffer.as_ptr().byte_add(record_index) };
                unsafe { &*(ptr as *const RecordDescriptor) }
            };

            let record_length: i32;
            {
                // aeron_get_volatile_i32(&mut record_length, &header.length);
                record_length = header.length.load(Ordering::Relaxed);
                compiler_fence(Ordering::SeqCst);
            }

            if record_length <= 0 {
                break;
            }

            bytes_read += aeron_align(record_length as usize, AERON_RB_ALIGNMENT);
            let msg_type_id: i32 = header.msg_type_id;

            if msg_type_id == AERON_RB_PADDING_MSG_TYPE_ID {
                continue;
            }

            messages_read += 1;
            // TODO: Return special type that increments head once dropped
            let data: &[u8] = {
                let index = aeron_rb_message_offset(record_index);
                let ptr = unsafe { self.buffer.as_ptr().byte_add(index) };
                unsafe {
                    slice::from_raw_parts(
                        ptr,
                        record_length as usize - AERON_RB_RECORD_HEADER_LENGTH,
                    )
                }
            };
            read_buffer.push((msg_type_id, data.to_vec()));
        }

        if bytes_read != 0 {
            // Set all the bytes read to 0 with memset
            let destination_ptr: *mut u8 = unsafe { self.buffer.as_ptr().byte_add(head_index) };
            unsafe { destination_ptr.write_bytes(0, bytes_read) };
            {
                // aeron_put_ordered_i64(self.descriptor.head_position(), head + bytes_read as i64);

                compiler_fence(Ordering::SeqCst);
                self.descriptor
                    .head_position
                    .store_atomic(head + bytes_read as i64, Ordering::Relaxed);
            }
        }

        // return vec
        read_buffer
    }
}
