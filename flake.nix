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

        tdlibTarget = builtins.getAttr system {
          aarch64-darwin = {
            name = "macos-aarch64";
            hash = "sha256-aKrS3wNsf5ajXpXKAEyx/T6bkYiWJGi5FDl9u2BDLOY=";
          };
          aarch64-linux = {
            name = "linux-aarch64";
            hash = "sha256-iy3hglwV423zngTTmNEuhTHUSZ4TqUpvSCzhT9G90qg=";
          };
          x86_64-linux = {
            name = "linux-x86_64";
            hash = "sha256-vRem/cv5wpPNoC3gf2sLKmVauAkU6ngNygAmtlOz0UQ=";
          };
        };
        tdlibBundle = pkgs.fetchzip {
          url = "https://github.com/FedericoBruzzone/tdlib-rs/releases/download/v1.4.0/tdlib-1.8.61-${tdlibTarget.name}.zip";
          inherit (tdlibTarget) hash;
        };

        platformPackages =
          lib.optionals pkgs.stdenv.isDarwin [pkgs.libiconv]
          # tdlib-rs links its bundled static TDLib against libc++ on Linux.
          ++ lib.optionals pkgs.stdenv.isLinux [pkgs.llvmPackages.libcxx];
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
          # tdlib-rs copies this hash-pinned archive into Cargo's build output.
          LOCAL_TDLIB_PATH = "${tdlibBundle}";
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
        };
      }
    );

    formatter = forEachSystem (system: nixpkgs.legacyPackages.${system}.alejandra);
  };
}
