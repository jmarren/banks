#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $DIR

mmdr -i "$DIR/architecture.md" -o "$DIR/architecture.svg"

