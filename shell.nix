{ pkgs ? import <nixpkgs> {} }:

let
  
  fdbVersion = "7.1.25";

  baseUrl = "https://github.com/apple/foundationdb/releases/download/${fdbVersion}/";

  fdbclients = pkgs.fetchurl {
    url = "${baseUrl}/foundationdb-clients_${fdbVersion}-1_amd64.deb";
    hash = "sha256-Z5HhQLSpEt1QwxzaZXbTYbX1XHfhV4hrsnkgA3LX/44";
  };

  fdbserver = pkgs.fetchurl {
    url = "${baseUrl}/foundationdb-server_${fdbVersion}-1_amd64.deb";
    hash = "sha256-g5bYMXsh4vt4bSA3h5S/o4sMQxzXoSKP6bEXHfX3DKQ";
  };

  fdb = pkgs.stdenv.mkDerivation rec {
    pname = "foundationdb";
    version = fdbVersion;

    nativeBuildInputs = [
      pkgs.autoPatchelfHook
    ];

    unpackPhase = ''
      mkdir clients
      cd clients
      ar x "${fdbclients}"
      tar -xzf data.tar.gz
      cd ..

      mkdir server
      cd server
      ar x "${fdbserver}"
      tar -xzf data.tar.gz
      cd ..
    '';

    installPhase = ''
      mkdir "$out"
      cp -r clients/usr/include "$out/include"
      install -D -m755 clients/usr/lib/libfdb_c.so "$out/lib/libfdb_c.so"
      for b in $(ls clients/usr/bin)
      do
        install -D -m755 "clients/usr/bin/$b" "$out/bin/$b"
      done
      install -D -m755 "server/usr/sbin/fdbserver" "$out/bin/fdbserver"
      install -D -m755 "server/usr/lib/foundationdb/fdbmonitor" "$out/bin/fdbmonitor"
    '';

  };

in
pkgs.mkShell {
  buildInputs = [fdb];
}