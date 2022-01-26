#!/bin/sh

if [ $# -lt 2 ]; then
    echo >&2 "Usage: $(basename "$0") <APP_NAME> [PROCESS TYPES]..."
    exit 1
fi

item() {
    echo "{ \"type\": \"$1\", \"docker_image\": \"$ID\" }"
}

join() {
    r=$(item "$1")
    shift

    while [ -n "$1" ]; do
        r="$r, $(item "$1")"
        shift
    done
    echo "$r"
}

readonly APP_NAME="$1"
readonly REGISTRY="registry.heroku.com/$APP_NAME"
shift

set -eu
docker build . -t "$REGISTRY"
docker push "$REGISTRY"

ID=$(docker inspect "$REGISTRY" --format='{{.Id}}')
curl --netrc -XPATCH "https://api.heroku.com/apps/$APP_NAME/formation" \
    -H"Content-Type: application/json" \
    -H"Accept: application/vnd.heroku+json; version=3.docker-releases" \
    -d "{ \"updates\": [$(join "$@")] }"
