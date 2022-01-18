#[derive(Debug)]
pub enum ErrorCode {
    FetchAccountFailure,
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
}
