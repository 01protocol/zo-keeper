use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::RpcFilterType,
};

use solana_account_decoder::UiAccountEncoding;

use solana_sdk::{
    account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey,
};

use crate::liquidator::error::ErrorCode;

pub fn get_accounts(
    client: &RpcClient,
    program_address: &Pubkey,
    data_size: u64,
) -> Result<Vec<(Pubkey, Account)>, ErrorCode> {
    // Make the config for getting accs of the right size
    let size_filter = RpcFilterType::DataSize(data_size);

    let config = RpcProgramAccountsConfig {
        filters: Some(vec![size_filter]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: None,
            commitment: Some(CommitmentConfig::finalized()),
        },
        with_context: Some(false),
    };

    let result =
        client.get_program_accounts_with_config(program_address, config);

    match result {
        Ok(accs) => Ok(accs),
        Err(_) => Err(ErrorCode::FetchAccountFailure),
    }
}
