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
        # tdlib-rs recursively copies LOCAL_TDLIB_PATH into OUT_DIR. Files copied
        # directly from the read-only Nix store retain read-only modes, so a
        # subsequent build-script rerun cannot overwrite its previous output.
        # Include the source hash and cache-layout version so a changed bundle
        # is never confused with an older writable copy.
        tdlibCacheKey =
          builtins.substring 0 20 (builtins.hashString "sha256"
            "writable-v1:${system}:${tdlibTarget.name}:${tdlibTarget.hash}");

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
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";

          # Keep one writable copy per user, platform, and fixed-output hash.
          # A mkdir lock serializes first use; a staging directory is published
          # with one rename, and the subshell trap removes interrupted copies.
          # Cargo may then rerun tdlib-rs's build script without inheriting the
          # Nix store's read-only file modes in OUT_DIR.
          shellHook = ''
            tdlib_cache_root="''${XDG_CACHE_HOME:-$HOME/.cache}/parse-bot/tdlib"
            tdlib_cache_path="$tdlib_cache_root/${tdlibTarget.name}-${tdlibCacheKey}"
            tdlib_ready_path="$tdlib_cache_path/.ready"
            tdlib_lock_path="$tdlib_cache_root/.${tdlibCacheKey}.lock"

            if [ ! -f "$tdlib_ready_path" ] || [ ! -d "$tdlib_cache_path/include" ] || [ ! -d "$tdlib_cache_path/lib" ]; then
              if ! (
                set -eu

                mkdir -p "$tdlib_cache_root"
                tdlib_staging_path=""
                tdlib_have_lock=0

                cleanup_tdlib_cache() {
                  if [ -n "$tdlib_staging_path" ] && [ -d "$tdlib_staging_path" ]; then
                    rm -rf -- "$tdlib_staging_path"
                  fi
                  if [ "$tdlib_have_lock" -eq 1 ]; then
                    rm -f -- "$tdlib_lock_path/owner"
                    rmdir "$tdlib_lock_path" 2>/dev/null || true
                  fi
                }
                trap cleanup_tdlib_cache EXIT
                trap 'exit 1' HUP INT TERM

                tdlib_wait_count=0
                while ! mkdir "$tdlib_lock_path" 2>/dev/null; do
                  if [ -f "$tdlib_ready_path" ] && [ -d "$tdlib_cache_path/include" ] && [ -d "$tdlib_cache_path/lib" ]; then
                    exit 0
                  fi

                  tdlib_lock_owner=""
                  if [ -r "$tdlib_lock_path/owner" ]; then
                    IFS= read -r tdlib_lock_owner < "$tdlib_lock_path/owner" || true
                  fi
                  case "x$tdlib_lock_owner" in
                    x|x*[!0-9]*)
                      # Allow the lock creator a moment to publish its PID. An
                      # empty abandoned lock is safe to remove with rmdir.
                      if [ "$tdlib_wait_count" -ge 50 ]; then
                        rmdir "$tdlib_lock_path" 2>/dev/null || true
                      fi
                      ;;
                    *)
                      if ! kill -0 "$tdlib_lock_owner" 2>/dev/null; then
                        rm -f -- "$tdlib_lock_path/owner"
                        rmdir "$tdlib_lock_path" 2>/dev/null || true
                      fi
                      ;;
                  esac

                  tdlib_wait_count=$((tdlib_wait_count + 1))
                  if [ "$tdlib_wait_count" -ge 3000 ]; then
                    echo "Timed out waiting for the parse-bot TDLib cache lock" >&2
                    exit 1
                  fi
                  sleep 0.1
                done

                tdlib_have_lock=1
                printf '%s\n' "''${BASHPID:-$$}" > "$tdlib_lock_path/owner"

                if [ -f "$tdlib_ready_path" ] && [ -d "$tdlib_cache_path/include" ] && [ -d "$tdlib_cache_path/lib" ]; then
                  exit 0
                fi

                # A cache without the ready marker was never atomically
                # published by this hook and can be rebuilt under the lock.
                if [ -e "$tdlib_cache_path" ]; then
                  rm -rf -- "$tdlib_cache_path"
                fi

                # SIGKILL or a machine shutdown cannot run EXIT traps. Reap
                # same-key staging directories only after taking the lock, so
                # interrupted copies do not accumulate across shell entries.
                for tdlib_abandoned_path in "$tdlib_cache_root/.${tdlibCacheKey}.tmp."*; do
                  if [ -d "$tdlib_abandoned_path" ]; then
                    rm -rf -- "$tdlib_abandoned_path"
                  fi
                done

                tdlib_staging_path="$(mktemp -d "$tdlib_cache_root/.${tdlibCacheKey}.tmp.XXXXXX")"
                cp -R "${tdlibBundle}/." "$tdlib_staging_path/"
                chmod -R u+rwX "$tdlib_staging_path"
                test -d "$tdlib_staging_path/include"
                test -d "$tdlib_staging_path/lib"
                printf '%s\n' "${tdlibBundle}" > "$tdlib_staging_path/.source"
                touch "$tdlib_staging_path/.ready"
                mv "$tdlib_staging_path" "$tdlib_cache_path"
                tdlib_staging_path=""
              ); then
                echo "Failed to prepare the writable TDLib cache" >&2
                unset LOCAL_TDLIB_PATH
              fi
            fi

            if [ -f "$tdlib_ready_path" ] && [ -d "$tdlib_cache_path/include" ] && [ -d "$tdlib_cache_path/lib" ]; then
              export LOCAL_TDLIB_PATH="$tdlib_cache_path"

              # Repair outputs produced before this cache existed. Their files
              # inherited read-only Nix-store modes, so the first build-script
              # rerun after LOCAL_TDLIB_PATH changes would otherwise still fail.
              tdlib_cargo_target="''${CARGO_TARGET_DIR:-$PWD/target}"
              (
                shopt -s nullglob
                for tdlib_output_path in \
                  "$tdlib_cargo_target"/*/build/tdlib-rs-*/out/tdlib \
                  "$tdlib_cargo_target"/*/*/build/tdlib-rs-*/out/tdlib; do
                  tdlib_output_marker="$tdlib_output_path/.parse-bot-writable-modes"
                  if [ ! -f "$tdlib_output_marker" ]; then
                    chmod -R u+rwX "$tdlib_output_path"
                    touch "$tdlib_output_marker"
                  fi
                done
              )
              unset tdlib_cargo_target
            fi

            unset tdlib_cache_root tdlib_cache_path tdlib_ready_path tdlib_lock_path
          '';
        };
      }
    );

    formatter = forEachSystem (system: nixpkgs.legacyPackages.${system}.alejandra);
  };
}
