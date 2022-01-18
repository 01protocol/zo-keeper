# zo-keeper

The `zo-keeper` (pronounced "zoo keeper") repository runs large scale
instructions that secure the 01 network, and allow it to operate in a
fully decentralized manner. Anyone can run any of the keepers in the
network.

The breakdown of the keepers are as follows:

```
zo-keeper
├── liquidator
├── consumer
│   ├── consume events
│   └── crank pnl
├── crank
│   ├── cache interest rates
│   ├── cache oracles
│   └── update perp funding rate
└── listener
    ├── funding updates
    ├── realized pnl changes
    ├── liquidations
    ├── bankruptcies
    └── trade fills
```

For more information, see the program's help menu, `zo-keeper --help`.

## Building

This repository has a submodule under the `/abi` directory, so make sure
the submodule is populated by running:

```bash
$ git submodule update --init
```

If `git status` shows nothing, then the submodule is up to date.
To build the project, simply run:

```bash
$ cargo build --release
```

The program will be built at `/target/release/zo-keeper`, or if
`--release` wasn't passed, then it will be at `/target/debug/zo-keeper`.

## Running

Running `/target/release/zo-keeper` with no argument prints the
program's help menu. There are a few arguments needed for all
subcommands, which can also be passed as environment variables.
Additionally, the project uses `dotenv` as well, so it's
recommended to copy `.env.example` to `.env` and configure it
appropriately, to avoid having to pass arguments every time.
