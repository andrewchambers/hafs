{ fetchurl, stdenv, autoPatchelfHook }:
let
  fdbVersion = "7.1.25";

  baseUrl = "https://github.com/apple/foundationdb/releases/download/${fdbVersion}/";

  fdbclients = fetchurl {
    url = "${baseUrl}/foundationdb-clients_${fdbVersion}-1_amd64.deb";
    hash = "sha256-Z5HhQLSpEt1QwxzaZXbTYbX1XHfhV4hrsnkgA3LX/44";
  };

  fdbserver = fetchurl {
    url = "${baseUrl}/foundationdb-server_${fdbVersion}-1_amd64.deb";
    hash = "sha256-g5bYMXsh4vt4bSA3h5S/o4sMQxzXoSKP6bEXHfX3DKQ";
  };
in
  stdenv.mkDerivation {
    pname = "foundationdb";
    
    version = fdbVersion;

    nativeBuildInputs = [
      autoPatchelfHook
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
      install -D -m755 "clients/usr/lib/foundationdb/backup_agent/backup_agent" "$out/libexec/backup_agent"
      mkdir -p "$out/lib/pkgconfig"
      
      cat <<EOF > "$out/lib/pkgconfig/foundationdb-client.pc"
      Name: foundationdb-client
      Description: FoundationDB c client
      Version: ${fdbVersion}

      Libs: -L$out/lib -lfdb_c
      Cflags: -I$out/include
      EOF

      install -D -m755 "server/usr/sbin/fdbserver" "$out/bin/fdbserver"
      install -D -m755 "server/usr/lib/foundationdb/fdbmonitor" "$out/bin/fdbmonitor"
    '';
  }