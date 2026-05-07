// Copyright 2022-2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

#![cfg(test)]

use eyre::{Result, bail};
use wasmer::{Engine, Instance, Module, Store, Value, imports};
use wasmer_compiler_cranelift::Cranelift;

#[test]
fn test_crate() -> Result<()> {
    // Adapted from https://docs.rs/wasmer/3.1.0/wasmer/index.html

    let source = std::fs::read("programs/pure/main.wat")?;

    let mut store = Store::default();
    let module = Module::new(&store, source)?;
    let imports = imports! {};
    let instance = Instance::new(&mut store, &module, &imports)?;

    let add_one = instance.exports.get_function("add_one")?;
    let result = add_one.call(&mut store, &[Value::I32(42)])?;
    assert_eq!(result[0], Value::I32(43));
    Ok(())
}

/// Loads soft-float.wasm into a plain Wasmer store (no NaN canonicalization),
/// mirroring how WAVM calls into the soft-float library via cross-module calls.
fn load_soft_float() -> Result<(Store, Instance)> {
    let wasm = std::fs::read("../../target/machines/latest/soft-float.wasm")?;
    let mut store = Store::default();
    let module = Module::new(&store, &wasm)?;
    let instance = Instance::new(&mut store, &module, &imports! {})?;
    Ok((store, instance))
}

/// Creates a Wasmer store backed by Cranelift with NaN canonicalization enabled,
/// matching the JIT configuration in `crates/jit/src/machine.rs`.
fn make_jit_store() -> Store {
    let mut compiler = Cranelift::new();
    compiler.canonicalize_nans(true);
    Store::new(Engine::from(compiler))
}

/// Compiles a tiny WASM module in the JIT store that wraps the given float binary
/// operation. Arguments and return value are passed as reinterpreted integers so
/// the caller can inspect raw NaN bit patterns.
fn jit_float_binop(op: &str) -> Result<(Store, Instance)> {
    let wat = format!(
        r#"(module
               (func (export "op") (param i32 i32) (result i32)
                   (f32.reinterpret_i32 (local.get 0))
                   (f32.reinterpret_i32 (local.get 1))
                   ({op})
                   (i32.reinterpret_f32)))"#
    );
    let wasm = wasmer::wat2wasm(wat.as_bytes())?.to_vec();
    let mut store = make_jit_store();
    let module = Module::new(&store, &wasm)?;
    let instance = Instance::new(&mut store, &module, &imports! {})?;
    Ok((store, instance))
}

fn jit_float64_binop(op: &str) -> Result<(Store, Instance)> {
    let wat = format!(
        r#"(module
               (func (export "op") (param i64 i64) (result i64)
                   (f64.reinterpret_i64 (local.get 0))
                   (f64.reinterpret_i64 (local.get 1))
                   ({op})
                   (i64.reinterpret_f64)))"#
    );
    let wasm = wasmer::wat2wasm(wat.as_bytes())?.to_vec();
    let mut store = make_jit_store();
    let module = Module::new(&store, &wasm)?;
    let instance = Instance::new(&mut store, &module, &imports! {})?;
    Ok((store, instance))
}

fn call_i32_binop(
    store: &mut Store,
    instance: &Instance,
    fname: &str,
    a: u32,
    b: u32,
) -> Result<u32> {
    let f = instance.exports.get_function(fname)?;
    let res = f.call(store, &[Value::I32(a as i32), Value::I32(b as i32)])?;
    match res[0] {
        Value::I32(x) => Ok(x as u32),
        _ => bail!("expected i32 result"),
    }
}

fn call_i64_binop(
    store: &mut Store,
    instance: &Instance,
    fname: &str,
    a: u64,
    b: u64,
) -> Result<u64> {
    let f = instance.exports.get_function(fname)?;
    let res = f.call(store, &[Value::I64(a as i64), Value::I64(b as i64)])?;
    match res[0] {
        Value::I64(x) => Ok(x as u64),
        _ => bail!("expected i64 result"),
    }
}

/// Confirms that WAVM (soft-float) and JIT diverge for f32.min when one operand
/// is a non-canonical NaN.
///
/// WAVM executes f32.min as a cross-module call to `wavm__f32_min` in soft-float.wasm,
/// which returns the NaN bits unchanged.  JIT runs native f32.min with
/// `canonicalize_nans(true)`, always producing the canonical quiet NaN 0x7FC00000.
///
/// This test asserts the correct (post-fix) behavior and currently fails.
#[test]
fn test_f32_min_nan_canonicalization() -> Result<()> {
    // 0x7F800001: signaling NaN (sNaN) — exponent=0xFF, mantissa=1, sign=0
    let snan: u32 = 0x7F800001;
    let one: u32 = 0x3F800000; // 1.0
    let canonical_nan: u32 = 0x7FC00000;

    let (mut sf_store, sf) = load_soft_float()?;
    let soft_bits = call_i32_binop(&mut sf_store, &sf, "wavm__f32_min", snan, one)?;

    let (mut jit_store, jit) = jit_float_binop("f32.min")?;
    let jit_bits = call_i32_binop(&mut jit_store, &jit, "op", snan, one)?;

    assert_eq!(
        jit_bits, canonical_nan,
        "JIT f32.min should return canonical NaN"
    );
    assert_eq!(
        soft_bits, canonical_nan,
        "soft-float f32.min with sNaN operand: got 0x{soft_bits:08x}, want canonical NaN 0x{canonical_nan:08x}"
    );
    Ok(())
}

/// Same as above but for f32.max.
#[test]
fn test_f32_max_nan_canonicalization() -> Result<()> {
    let snan: u32 = 0x7F800001;
    let one: u32 = 0x3F800000;
    let canonical_nan: u32 = 0x7FC00000;

    let (mut sf_store, sf) = load_soft_float()?;
    let soft_bits = call_i32_binop(&mut sf_store, &sf, "wavm__f32_max", snan, one)?;

    let (mut jit_store, jit) = jit_float_binop("f32.max")?;
    let jit_bits = call_i32_binop(&mut jit_store, &jit, "op", snan, one)?;

    assert_eq!(
        jit_bits, canonical_nan,
        "JIT f32.max should return canonical NaN"
    );
    assert_eq!(
        soft_bits, canonical_nan,
        "soft-float f32.max with sNaN operand: got 0x{soft_bits:08x}, want canonical NaN 0x{canonical_nan:08x}"
    );
    Ok(())
}

/// Confirms divergence for f64.min with a non-canonical NaN.
#[test]
fn test_f64_min_nan_canonicalization() -> Result<()> {
    // 0x7FF0000000000001: signaling NaN for f64
    let snan: u64 = 0x7FF0000000000001;
    let one: u64 = 0x3FF0000000000000; // 1.0
    let canonical_nan: u64 = 0x7FF8000000000000;

    let (mut sf_store, sf) = load_soft_float()?;
    let soft_bits = call_i64_binop(&mut sf_store, &sf, "wavm__f64_min", snan, one)?;

    let (mut jit_store, jit) = jit_float64_binop("f64.min")?;
    let jit_bits = call_i64_binop(&mut jit_store, &jit, "op", snan, one)?;

    assert_eq!(
        jit_bits, canonical_nan,
        "JIT f64.min should return canonical NaN"
    );
    assert_eq!(
        soft_bits, canonical_nan,
        "soft-float f64.min with sNaN operand: got 0x{soft_bits:016x}, want canonical NaN 0x{canonical_nan:016x}"
    );
    Ok(())
}

/// Confirms divergence for f64.max with a non-canonical NaN.
#[test]
fn test_f64_max_nan_canonicalization() -> Result<()> {
    let snan: u64 = 0x7FF0000000000001;
    let one: u64 = 0x3FF0000000000000;
    let canonical_nan: u64 = 0x7FF8000000000000;

    let (mut sf_store, sf) = load_soft_float()?;
    let soft_bits = call_i64_binop(&mut sf_store, &sf, "wavm__f64_max", snan, one)?;

    let (mut jit_store, jit) = jit_float64_binop("f64.max")?;
    let jit_bits = call_i64_binop(&mut jit_store, &jit, "op", snan, one)?;

    assert_eq!(
        jit_bits, canonical_nan,
        "JIT f64.max should return canonical NaN"
    );
    assert_eq!(
        soft_bits, canonical_nan,
        "soft-float f64.max with sNaN operand: got 0x{soft_bits:016x}, want canonical NaN 0x{canonical_nan:016x}"
    );
    Ok(())
}

/// Confirms that soft-float produces a wrong (negative) default NaN for operations
/// that generate NaN from scratch (e.g. ∞ + -∞).  The SoftFloat 8086 variant uses
/// defaultNaNF32UI = 0xFFC00000 (negative NaN), while JIT produces 0x7FC00000.
#[test]
fn test_f32_add_generated_nan_canonicalization() -> Result<()> {
    let pos_inf: u32 = 0x7F800000; // +∞
    let neg_inf: u32 = 0xFF800000; // -∞
    let canonical_nan: u32 = 0x7FC00000;

    let (mut sf_store, sf) = load_soft_float()?;
    let soft_bits = call_i32_binop(&mut sf_store, &sf, "wavm__f32_add", pos_inf, neg_inf)?;

    let (mut jit_store, jit) = jit_float_binop("f32.add")?;
    let jit_bits = call_i32_binop(&mut jit_store, &jit, "op", pos_inf, neg_inf)?;

    assert_eq!(
        jit_bits, canonical_nan,
        "JIT f32.add(+∞, -∞) should return canonical NaN"
    );
    assert_eq!(
        soft_bits, canonical_nan,
        "soft-float f32.add(+∞, -∞): got 0x{soft_bits:08x}, want canonical NaN 0x{canonical_nan:08x}"
    );
    Ok(())
}

/// Same divergence for f64 generated NaN.
#[test]
fn test_f64_add_generated_nan_canonicalization() -> Result<()> {
    let pos_inf: u64 = 0x7FF0000000000000; // +∞
    let neg_inf: u64 = 0xFFF0000000000000; // -∞
    let canonical_nan: u64 = 0x7FF8000000000000;

    let (mut sf_store, sf) = load_soft_float()?;
    let soft_bits = call_i64_binop(&mut sf_store, &sf, "wavm__f64_add", pos_inf, neg_inf)?;

    let (mut jit_store, jit) = jit_float64_binop("f64.add")?;
    let jit_bits = call_i64_binop(&mut jit_store, &jit, "op", pos_inf, neg_inf)?;

    assert_eq!(
        jit_bits, canonical_nan,
        "JIT f64.add(+∞, -∞) should return canonical NaN"
    );
    assert_eq!(
        soft_bits, canonical_nan,
        "soft-float f64.add(+∞, -∞): got 0x{soft_bits:016x}, want canonical NaN 0x{canonical_nan:016x}"
    );
    Ok(())
}
