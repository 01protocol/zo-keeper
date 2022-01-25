use crate::Error;
use anchor_client::{
    anchor_lang::{prelude::AccountLoader, Owner, ZeroCopy},
    solana_client::{
        rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    },
    solana_sdk::{
        account::Account, account_info::AccountInfo,
        commitment_config::CommitmentConfig, pubkey::Pubkey,
    },
};
use solana_account_decoder::UiAccountEncoding;

fn load_account<'a, T>(key: &'a Pubkey, account: &'a mut Account) -> T
where
    T: ZeroCopy + Owner,
{
    let account_info: AccountInfo<'_> = (key, account).into();
    let loader: AccountLoader<'_, T> =
        AccountLoader::try_from(&account_info).unwrap();
    let account = *loader.load().unwrap();
    account
}

pub fn load_program_accounts<T>(
    client: &RpcClient,
) -> Result<Vec<(Pubkey, T)>, Error>
where
    T: ZeroCopy + Owner,
{
    let config = RpcProgramAccountsConfig {
        filters: Some(vec![
            RpcFilterType::DataSize((8 + std::mem::size_of::<T>()) as u64),
            RpcFilterType::Memcmp(Memcmp {
                offset: 0,
                bytes: MemcmpEncodedBytes::Bytes(T::discriminator().into()),
                encoding: None,
            }),
        ]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: None,
            commitment: Some(CommitmentConfig::finalized()),
        },
        with_context: Some(false),
    };

    client
        .get_program_accounts_with_config(&zo_abi::ID, config)
        .map(|v| {
            v.into_iter()
                .map(|(k, mut a)| (k, load_account::<T>(&k, &mut a)))
                .collect()
        })
        .map_err(Into::into)
}
