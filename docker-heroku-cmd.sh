#!/bin/sh

run() {
    exec ./target/release/zo-keeper "$@"
}

case "$DYNO" in
    crank.*) run crank ;;
    consumer.*) run consumer ;;
    listener.*) run listener ;;
    liquidator.*) run liquidator ;;
esac
