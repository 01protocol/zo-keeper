./target/devnet/release/zo-keeper \
    --rpc-url "https://psytrbhymqlkfrhudd.dev.genesysgo.net:8899/" \
    --ws-url "wss://psytrbhymqlkfrhudd.dev.genesysgo.net:8900/" \
    liquidator --worker-count 1 --worker-index 0 | tee $1
