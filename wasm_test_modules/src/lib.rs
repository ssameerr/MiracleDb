// Numeric test functions (existing)
#[no_mangle]
pub extern "C" fn add(a: i64, b: i64) -> i64 {
    a + b
}

#[no_mangle]
pub extern "C" fn sum3(a: f64, b: f64, c: f64) -> f64 {
    a + b + c
}

// Memory allocator for WASM linear memory (required for string support)
#[no_mangle]
pub extern "C" fn allocate(size: i32) -> i32 {
    let mut buf = Vec::<u8>::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();
    core::mem::forget(buf); // Prevent deallocation
    (ptr as usize) as i32
}

#[no_mangle]
pub extern "C" fn deallocate(ptr: i32, size: i32) {
    unsafe {
        let _ = Vec::from_raw_parts(ptr as *mut u8, 0, size as usize);
        // Dropped automatically
    }
}

// String test functions

/// Uppercase a string
#[no_mangle]
pub extern "C" fn uppercase(ptr: i32, len: i32) -> (i32, i32) {
    let input = unsafe {
        let slice = core::slice::from_raw_parts(ptr as *const u8, len as usize);
        core::str::from_utf8_unchecked(slice)
    };

    let result = input.to_uppercase();
    let result_bytes = result.as_bytes();
    let result_len = result_bytes.len() as i32;

    let result_ptr = allocate(result_len);
    unsafe {
        core::ptr::copy_nonoverlapping(
            result_bytes.as_ptr(),
            result_ptr as *mut u8,
            result_len as usize,
        );
    }

    (result_ptr, result_len)
}

/// Lowercase a string
#[no_mangle]
pub extern "C" fn lowercase(ptr: i32, len: i32) -> (i32, i32) {
    let input = unsafe {
        let slice = core::slice::from_raw_parts(ptr as *const u8, len as usize);
        core::str::from_utf8_unchecked(slice)
    };

    let result = input.to_lowercase();
    let result_bytes = result.as_bytes();
    let result_len = result_bytes.len() as i32;

    let result_ptr = allocate(result_len);
    unsafe {
        core::ptr::copy_nonoverlapping(
            result_bytes.as_ptr(),
            result_ptr as *mut u8,
            result_len as usize,
        );
    }

    (result_ptr, result_len)
}

/// Concatenate two strings
#[no_mangle]
pub extern "C" fn concat(ptr1: i32, len1: i32, ptr2: i32, len2: i32) -> (i32, i32) {
    let str1 = unsafe {
        let slice = core::slice::from_raw_parts(ptr1 as *const u8, len1 as usize);
        core::str::from_utf8_unchecked(slice)
    };

    let str2 = unsafe {
        let slice = core::slice::from_raw_parts(ptr2 as *const u8, len2 as usize);
        core::str::from_utf8_unchecked(slice)
    };

    let result = format!("{}{}", str1, str2);
    let result_bytes = result.as_bytes();
    let result_len = result_bytes.len() as i32;

    let result_ptr = allocate(result_len);
    unsafe {
        core::ptr::copy_nonoverlapping(
            result_bytes.as_ptr(),
            result_ptr as *mut u8,
            result_len as usize,
        );
    }

    (result_ptr, result_len)
}

/// Substring (mixed types: string, i64, i64 -> string)
#[no_mangle]
pub extern "C" fn substring(ptr: i32, len: i32, start: i64, count: i64) -> (i32, i32) {
    let input = unsafe {
        let slice = core::slice::from_raw_parts(ptr as *const u8, len as usize);
        core::str::from_utf8_unchecked(slice)
    };

    let start_idx = start.max(0) as usize;
    let end_idx = (start + count).max(0) as usize;

    let result = if start_idx < input.len() {
        let end = end_idx.min(input.len());
        &input[start_idx..end]
    } else {
        ""
    };

    let result_bytes = result.as_bytes();
    let result_len = result_bytes.len() as i32;

    let result_ptr = allocate(result_len);
    unsafe {
        core::ptr::copy_nonoverlapping(
            result_bytes.as_ptr(),
            result_ptr as *mut u8,
            result_len as usize,
        );
    }

    (result_ptr, result_len)
}

/// String length (string -> i64)
#[no_mangle]
pub extern "C" fn str_length(ptr: i32, len: i32) -> i64 {
    let input = unsafe {
        let slice = core::slice::from_raw_parts(ptr as *const u8, len as usize);
        core::str::from_utf8_unchecked(slice)
    };

    input.chars().count() as i64
}

/// String repeat (string + i64 -> string)
#[no_mangle]
pub extern "C" fn repeat_string(ptr: i32, len: i32, count: i64) -> (i32, i32) {
    let input = unsafe {
        let slice = core::slice::from_raw_parts(ptr as *const u8, len as usize);
        core::str::from_utf8_unchecked(slice)
    };

    let times = count.max(0) as usize;
    let result = input.repeat(times);
    let result_bytes = result.as_bytes();
    let result_len = result_bytes.len() as i32;

    let result_ptr = allocate(result_len);
    unsafe {
        core::ptr::copy_nonoverlapping(
            result_bytes.as_ptr(),
            result_ptr as *mut u8,
            result_len as usize,
        );
    }

    (result_ptr, result_len)
}
