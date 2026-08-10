{
  description = "Rust development environment for the parse_bot project";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = {nixpkgs, ...}: let
    systems = [
      "aarch64-darwin"
      "aarch64-linux"
      "x86_64-linux"
    ];

    forEachSystem = nixpkgs.lib.genAttrs systems;
  in {
    devShells = forEachSystem (
      system: let
        pkgs = import nixpkgs {inherit system;};
        inherit (pkgs) lib;

        telegramBotApiAvailable = lib.meta.availableOn pkgs.stdenv.hostPlatform pkgs.telegram-bot-api;

        platformPackages =
          lib.optionals telegramBotApiAvailable [pkgs.telegram-bot-api]
          ++ lib.optionals pkgs.stdenv.isDarwin [pkgs.libiconv];
      in {
        default = pkgs.mkShell {
          packages =
            (with pkgs; [
              # Rust toolchain and editor support.
              rustc
              cargo
              clippy
              rustfmt
              rust-analyzer

              # Common Cargo development tools.
              cargo-audit
              cargo-edit
              cargo-nextest
              cargo-watch

              # Native dependencies and diagnostics.
              pkg-config
              openssl
              sqlite
              ffmpeg-headless
              cacert
              git
              curl
              jq
              alejandra
            ])
            ++ platformPackages;

          RUST_BACKTRACE = "1";
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
        };
      }
    );

    formatter = forEachSystem (system: nixpkgs.legacyPackages.${system}.alejandra);
  };
}
