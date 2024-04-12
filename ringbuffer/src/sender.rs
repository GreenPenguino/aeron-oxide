use std::{
    ptr::{self, NonNull},
    sync::atomic::{compiler_fence, Ordering},
};

use crate::{
    aeron_align, aeron_put_ordered_i32, aeron_rb_invalid_msg_type_id, aeron_rb_message_offset,
    descriptor::SenderDescriptor, RecordDescriptor, AERON_RB_ALIGNMENT,
    AERON_RB_PADDING_MSG_TYPE_ID, AERON_RB_RECORD_HEADER_LENGTH,
};

// IDEA: capacity could be a const generic (<const N: usize>)
pub struct Sender {
    pub(crate) buffer: NonNull<u8>,
    pub(crate) capacity: usize,
    pub(crate) descriptor: SenderDescriptor,
    pub(crate) max_message_length: usize,
}

impl Sender {
    pub unsafe fn new(buffer: *mut u8, length: usize) -> Result<Self, ()> {
        todo!()
    }

    pub fn send(&mut self, msg_type_id: i32, msg: &[u8]) -> Result<(), ()> {
        if msg.len() > self.max_message_length || aeron_rb_invalid_msg_type_id(msg_type_id) {
            return Err(());
        }

        let record_length: usize = msg.len() + AERON_RB_RECORD_HEADER_LENGTH;
        let record_index = self.claim_capacity(record_length)?;

        let record_header: *mut RecordDescriptor =
            unsafe { self.buffer.as_ptr().byte_add(record_index as usize) }
                as *mut RecordDescriptor;

        {
            // Probably wrong, should likely be an atomic (lets see if loom caches this).
            // aeron_put_ordered_i32(&unsafe { &*record_header }.length, -(record_length as i32));
            compiler_fence(Ordering::SeqCst);
            unsafe { &*record_header }
                .length
                .store(-(record_length as i32), Ordering::Relaxed);
        }

        let index = aeron_rb_message_offset(record_index as usize);
        let destination_ptr: *mut u8 = unsafe { self.buffer.as_ptr().byte_add(index) };
        unsafe { ptr::copy_nonoverlapping(msg.as_ptr(), destination_ptr, msg.len()) };

        unsafe { &mut *record_header }.msg_type_id = msg_type_id;
        aeron_put_ordered_i32(&unsafe { &*record_header }.length, record_length as i32);

        Ok(())
    }

    // TODO: check if result can be changed to u32
    fn claim_capacity(&mut self, record_length: usize) -> Result<i32, ()> {
        let required_capacity: usize = aeron_align(record_length, AERON_RB_ALIGNMENT);
        let mask: usize = self.capacity - 1;
        let mut head: i64;
        let mut tail: i64;
        let mut tail_index: usize;
        let mut padding: usize;

        {
            // aeron_get_volatile_i64(&mut head, self.descriptor.head_cache_position());

            head = self
                .descriptor
                .head_cache_position
                .load_atomic(Ordering::Relaxed);
            compiler_fence(Ordering::SeqCst);
        }

        loop {
            {
                // aeron_get_volatile_i64(&mut tail, self.descriptor.tail_position());
                tail = self.descriptor.tail_position.load_atomic(Ordering::Relaxed);
                compiler_fence(Ordering::SeqCst);
            }

            let available_capacity = self.capacity - (tail as usize - head as usize);
            debug_assert!(available_capacity <= self.capacity);

            if required_capacity > available_capacity {
                {
                    // aeron_get_volatile_i64(&mut head, self.descriptor.head_position());
                    head = self.descriptor.head_position.load_atomic(Ordering::Relaxed);
                    compiler_fence(Ordering::SeqCst);
                }

                if required_capacity > (self.capacity - (tail as usize - head as usize)) {
                    return Err(());
                }

                {
                    // aeron_put_ordered_i64(self.descriptor.head_cache_position(), head);

                    // Probably wrong, should be Release
                    compiler_fence(std::sync::atomic::Ordering::SeqCst);
                    self.descriptor
                        .head_cache_position
                        .store_atomic(head, Ordering::Relaxed);
                }
            }

            padding = 0;
            tail_index = tail as usize & mask;
            let to_buffer_end_length = self.capacity - tail_index;

            if required_capacity > to_buffer_end_length {
                // The message doesn't fit between tail index and end of buffer.

                let mut head_index = head as usize & mask;

                if required_capacity > head_index {
                    // The message doesn't fit between start of buffer and **cached** head index.

                    {
                        // aeron_get_volatile_i64(&mut head, self.descriptor.head_position());

                        // Should probably be Release - Acquire pair
                        // equivalent to aeron_get_volatile
                        head = self.descriptor.head_position.load_atomic(Ordering::Relaxed);
                        compiler_fence(std::sync::atomic::Ordering::SeqCst);
                    }
                    head_index = head as usize & mask;

                    if required_capacity > head_index {
                        // The message doesn't fit between start of buffer and head index.
                        return Err(());
                    }

                    {
                        // aeron_put_ordered_i64(self.descriptor.head_cache_position(), head);

                        // equivalent to aeron_put_ordered
                        // Should probably be Release - Acquire pair
                        compiler_fence(std::sync::atomic::Ordering::SeqCst);
                        self.descriptor
                            .head_cache_position
                            .store_atomic(head, Ordering::Relaxed);
                    }
                }

                padding = to_buffer_end_length;
            }

            // Exit condition
            // Probably wrong memory ordering
            if self.descriptor.tail_position.cas(
                tail,
                tail + required_capacity as i64 + padding as i64,
                Ordering::Relaxed,
            ) {
                break;
            }
        }

        if padding != 0 {
            let record_header: &mut RecordDescriptor = {
                let ptr: *mut u8 = unsafe { self.buffer.as_ptr().byte_add(tail_index) };
                unsafe { &mut *(ptr as *mut RecordDescriptor) }
            };

            // TODO: get rid of aeron_put_ordered_i32
            aeron_put_ordered_i32(&mut record_header.length, -(padding as i32));
            record_header.msg_type_id = AERON_RB_PADDING_MSG_TYPE_ID;
            aeron_put_ordered_i32(&record_header.length, padding as i32);
            tail_index = 0;
        }

        Ok(tail_index as i32)
    }
}
