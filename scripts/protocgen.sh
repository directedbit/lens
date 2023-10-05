#!/usr/bin/env bash

set -eox pipefail

echo "Generating gogo proto code"
# RD: Unsure if I need the third_party dir
#proto_dirs=$(find ./proto ./third_party -path -prune -o -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq)
proto_dirs=$(find ./proto -path -prune -o -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq)
for dir in $proto_dirs; do
  for file in $(find "${dir}" -maxdepth 1 -name '*.proto'); do
    buf generate --template proto/buf.gen.gogo.yaml $file
    # RD. Below gives errors that I haven't tried to debug
    # buf generate --template proto/buf.gen.gogo.yaml --gocosmos_out=plugins=interfacetype+grpc,\
    #Mgoogle/protobuf/any.proto=github.com/cosmos/cosmos-sdk/codec/types:. $file
  done
done

# move proto files to the right places
#
# Note: Proto files are suffixed with the current binary version.
cp -rv github.com/strangelove-ventures/lens/* ./
rm -rf github.com
