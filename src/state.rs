use anchor_client::{
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig, pubkey::Pubkey,
        signer::keypair::Keypair,
    },
    Client, Cluster, Program,
};

pub struct AppState {
    payer: Keypair,
    commitment: CommitmentConfig,
    pub cluster: Cluster,
    pub rpc: RpcClient,
    pub zo_state: zo_abi::State,
    pub zo_cache: zo_abi::Cache,
    pub zo_state_pubkey: Pubkey,
    pub zo_cache_pubkey: Pubkey,
    pub zo_state_signer_pubkey: Pubkey,
}

impl AppState {
    pub fn new(
        cluster: Cluster,
        commitment: CommitmentConfig,
        payer: Keypair,
    ) -> Self {
        let program = Client::new_with_options(
            cluster.clone(),
            std::rc::Rc::new(Keypair::from_bytes(&payer.to_bytes()).unwrap()),
            commitment.clone(),
        )
        .program(zo_abi::ID);

        let rpc = program.rpc();
        let zo_state_pubkey = zo_abi::ZO_STATE_ID;
        let zo_state: zo_abi::State = program.account(zo_state_pubkey).unwrap();
        let zo_cache: zo_abi::Cache = program.account(zo_state.cache).unwrap();
        let (zo_state_signer_pubkey, state_signer_nonce) =
            Pubkey::find_program_address(
                &[zo_state_pubkey.as_ref()],
                &zo_abi::ID,
            );

        if state_signer_nonce != zo_state.signer_nonce {
            panic!("Invalid state signer nonce");
        }

        Self {
            payer,
            commitment: CommitmentConfig::confirmed(),
            cluster,
            rpc,
            zo_state,
            zo_cache,
            zo_state_pubkey,
            zo_cache_pubkey: zo_state.cache,
            zo_state_signer_pubkey,
        }
    }

    pub fn payer(&self) -> Pubkey {
        use anchor_client::solana_sdk::signer::Signer;
        self.payer.pubkey()
    }

    pub fn payer_key(&self) -> &Keypair {
        &self.payer
    }

    pub fn client(&self) -> Client {
        Client::new_with_options(
            self.cluster.clone(),
            std::rc::Rc::new(
                Keypair::from_bytes(&self.payer.to_bytes()).unwrap(),
            ),
            self.commitment.clone(),
        )
    }

    pub fn program(&self) -> Program {
        self.client().program(zo_abi::ID)
    }

    pub fn iter_markets(
        &self,
    ) -> impl Iterator<Item = &zo_abi::PerpMarketInfo> {
        self.zo_state
            .perp_markets
            .iter()
            .filter(|market| market.dex_market != Pubkey::default())
    }

    pub fn load_dex_markets(
        &self,
    ) -> Result<Vec<(String, zo_abi::dex::ZoDexMarket)>, crate::Error> {
        self.iter_markets()
            .map(|m| {
                Ok((
                    m.symbol.into(),
                    *zo_abi::dex::ZoDexMarket::deserialize(
                        &self.rpc.get_account_data(&m.dex_market)?,
                    )
                    .unwrap(),
                ))
            })
            .collect()
    }

    pub fn iter_oracles(&self) -> impl Iterator<Item = &zo_abi::OracleCache> {
        self.zo_cache.oracles.iter().filter(|x| !x.symbol.is_nil())
    }

    pub fn iter_collaterals(
        &self,
    ) -> impl Iterator<Item = &zo_abi::CollateralInfo> {
        self.zo_state
            .collaterals
            .iter()
            .filter(|x| x.mint != Pubkey::default())
    }
}
