#!/usr/bin/env bash
set -eu

dir=/usr/local/lib/gost
ref=v3.0.3
openssl version | grep -q '^OpenSSL 1' && ref=openssl_1_1_1

rm -rf "$dir"
git -c advice.detachedHead=false clone -q --depth 1 --branch "$ref" --recurse-submodules --shallow-submodules \
    https://github.com/gost-engine/engine.git "$dir"

# The engine prints this to stdout when loaded twice in one process, which breaks programs that use stdout as a protocol (e.g. git
# over https on RHEL 8)
sed -i 's|printf("GOST engine already loaded\\n");||' "$dir/gost_eng.c"

cmake -S "$dir" -B "$dir/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$dir/build" -j "$(nproc)" --target gost_engine $([ "$ref" = openssl_1_1_1 ] || echo gost_prov) >/dev/null

cat > /openssl-gost-engine.cnf <<EOF
openssl_conf = init
[init]
engines = engines
[engines]
gost = gost
[gost]
dynamic_path = $dir/build/bin/gost.so
default_algorithms = ALL
EOF

cat > /openssl-gost-provider-only.cnf <<EOF
openssl_conf = init
[init]
providers = providers
[providers]
gostprov = gostprov
[gostprov]
module = $dir/build/bin/gostprov.so
activate = 1
EOF
