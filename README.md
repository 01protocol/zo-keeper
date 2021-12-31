# zo-keeper

The zo-keeper (pronounced "zoo keeper") repository runs large scale instructions that secure the 01 network, and allow it to operate in a fully decentralized manner. 

Anyone can run any of the keepers in the network.

The breakdown of the keepers are as follows:
```
Zo Keepers 🦒
└───Consumers 🍽
│   │   Events
│   │   PnL
│
└───Liquidators 💦
|
└───Cranks ⚙️
│   │   Cache IR
│   │   Cache Oracle
│   │   Update Funding
|
└───Loggers 🪵️
│   │   Funding Logger
│   │   Realized PnL Logger
│   │   Liquidation Logger
│   │   Deposit/ Withdraw Logger
│   │   Trade fills Logger
```


## Installation

1. Setup submodule dependency
```bash
git submodule update --init --recursive
```

## Usage 

Copy `.env.example` to `.env` and configure accordingly. Then, build the
project using `cargo` follow instructions in the help menu.
