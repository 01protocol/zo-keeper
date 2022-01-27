#[derive(Debug)]
pub enum ErrorCode {
    MathFailure,
    #[allow(dead_code)]
    InexistentControl,
    LockFailure,
    CollateralFailure,
    NoCollateral,
    NoPositions,
    LiquidationFailure,
    SwapError,
    TimeoutExceeded,
    CancelFailure,
    SettlementFailure,
    NoAsks,
    UnrecoverableTransactionError,
    LiquidationOverExposure,
}
