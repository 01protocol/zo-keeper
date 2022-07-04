#!/bin/sh

run() {
    exec ./target/release/zo-keeper "$@"
}

case "$DYNO" in
    crank.*) run crank ;;
    consumer.*) run consumer ;;
    liquidator.*) run liquidator ;;
    recorder.*) run recorder ;;
    trigger.*) run trigger ;;
esac
