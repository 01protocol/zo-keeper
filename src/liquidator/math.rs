#![allow(non_snake_case)]

use crate::liquidator::error::ErrorCode::{self, *};
use az::{CheckedAs, CheckedCast};
use fixed::types::I80F48;
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};

pub trait SafeOp<T>
where
    Self: Sized,
{
    fn safe_add(&self, x: T) -> Result<Self, ErrorCode>;
    fn safe_sub(&self, x: T) -> Result<Self, ErrorCode>;
    fn safe_mul(&self, x: T) -> Result<Self, ErrorCode>;
    fn safe_div(&self, x: T) -> Result<Self, ErrorCode>;
}

macro_rules! safe_impl {
    ( $f:ident, $g:ident ) => {
        fn $f(&self, x: U) -> Result<Self, ErrorCode> {
            x.checked_as().and_then(|x| self.$g(&x)).ok_or(MathFailure)
        }
    };
}

impl<T, U> SafeOp<U> for T
where
    T: CheckedAdd + CheckedSub + CheckedMul + CheckedDiv,
    U: CheckedCast<T>,
{
    safe_impl!(safe_add, checked_add);
    safe_impl!(safe_sub, checked_sub);
    safe_impl!(safe_mul, checked_mul);
    safe_impl!(safe_div, checked_div);
}

// I80F48
pub fn safe_add_i80f48(a: I80F48, b: I80F48) -> I80F48 {
    let c = a.checked_add(b).ok_or(MathFailure);
    c.unwrap()
}

pub fn safe_sub_i80f48(a: I80F48, b: I80F48) -> I80F48 {
    let c = a.checked_sub(b).ok_or(MathFailure);
    c.unwrap()
}

pub fn safe_mul_i80f48(a: I80F48, b: I80F48) -> I80F48 {
    let c = a.checked_mul(b).ok_or(MathFailure);
    c.unwrap()
}

pub fn safe_div_i80f48(a: I80F48, b: I80F48) -> I80F48 {
    let c = a.checked_div(b).ok_or(MathFailure);
    c.unwrap()
}
