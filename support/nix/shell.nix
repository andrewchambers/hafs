let
  pkgs = (import <nixpkgs>) {};
in
  pkgs.mkShell {
    buildInputs = [
      pkgs.go
      pkgs.gotools
      ((pkgs.callPackage ./fdb.nix) {})
    ];
  }