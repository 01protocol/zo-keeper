./target/mainnet/release/zo-keeper --rpc-url "https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc" --ws-url "wss://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/
" liquidator --worker-count 1 --worker-index 0 | tee $1
