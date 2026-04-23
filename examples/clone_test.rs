/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use qubit_lock::{
    Lock,
    lock::ArcStdMutex,
};

fn main() {
    println!("Testing ArcStdMutex Clone functionality...");

    // Test Clone with different types
    let string_mutex = ArcStdMutex::new(String::from("hello"));
    let string_clone = string_mutex.clone();
    string_clone.write(|s| s.push_str(" world"));
    let result = string_mutex.read(|s| s.clone());
    assert_eq!(result, "hello world");
    println!("✓ String clone test passed");

    // Test Clone with Vec
    let vec_mutex = ArcStdMutex::new(vec![1, 2, 3]);
    let vec_clone = vec_mutex.clone();
    vec_clone.write(|v| v.push(4));
    let result = vec_mutex.read(|v| v.clone());
    assert_eq!(result, vec![1, 2, 3, 4]);
    println!("✓ Vec clone test passed");

    // Test Clone with Option
    let option_mutex = ArcStdMutex::new(Some(42));
    let option_clone = option_mutex.clone();
    option_clone.write(|opt| *opt = Some(84));
    let result = option_mutex.read(|opt| *opt);
    assert_eq!(result, Some(84));
    println!("✓ Option clone test passed");

    println!("All Clone tests passed! 🎉");
}
