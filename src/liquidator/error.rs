#[derive(Debug)]
pub enum ErrorCode {
    MathFailure,
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
