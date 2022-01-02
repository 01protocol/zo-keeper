#!/bin/sh
set -eu
heroku container:push --recursive
heroku container:release crank listener consumer
