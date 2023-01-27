let
  pkgs = (import <nixpkgs>) {};
in
  pkgs.mkShell {
    buildInputs = [
      pkgs.go
      ((pkgs.callPackage ./foundationdb.nix) {})
    ];
  }