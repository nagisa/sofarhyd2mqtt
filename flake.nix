{
    inputs = {
        nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
        flake-utils.url = "github:numtide/flake-utils";
    };
    outputs = { self, nixpkgs, flake-utils }: let
        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        package = pkgs: pkgs.rustPlatform.buildRustPackage {
            inherit (cargoToml.package) name version;
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            doCheck = false;
        };
        flakeForSystem = system: let
            pkgs = nixpkgs.legacyPackages.${system};
        in {
            devShells.default = with nixpkgs.legacyPackages.${system}; mkShell {
                buildInputs = [
                    rustup
                    gcc
                    mold
                    sccache
                ];
                RUSTC_WRAPPER = "sccache";
                RUSTFLAGS = "-C link-arg=-fuse-ld=mold";
            };
            packages.default = package pkgs;
        };
    in (flake-utils.lib.eachDefaultSystem (system: flakeForSystem system)) // rec {
        overlay = final: prev: { sofarhyd2mqtt = package final; };
        nixosModules.default = { config, lib, pkgs, utils, ... }: let
            cfg = config.services.sofarhyd2mqtt;
        in with lib; {
            options.services.sofarhyd2mqtt = {
                enable = mkEnableOption "Enable the sofarhyd2mqtt service";
                package = mkOption {
                    description = "The sofarhyd2mqtt package to use";
                    type = types.package;
                    default = pkgs.sofarhyd2mqtt;
                };
                flags = mkOption {
                    description = ''`sofarhyd2mqtt mqtt` CLI flags to pass into the service'';
                    type = types.listOf types.str;
                };
                logFilter = mkOption {
                    description = ''Logging filter for sofarhyd2mqtt service'';
                    type = types.str;
                    default = "warn";
                };
            };
            config = mkIf cfg.enable {
                nixpkgs.overlays = [ overlay ];
                systemd.services.sofarhyd2mqtt = {
                    description = "sofarhyd2mqtt proxy";
                    wants = [ "network.target" ];
                    wantedBy = [ "multi-user.target" ];
                    unitConfig.StartLimitIntervalSec = "0s";
                    environment = {
                        SOFARHYD2MQTT_LOG = cfg.logFilter;
                    };
                    serviceConfig = {
                        Restart = "always";
                        RestartSec = 5;
                        ExecStart = utils.escapeSystemdExecArgs ([
                            "${cfg.package}/bin/sofarhyd2mqtt" "mqtt"
                        ] ++ cfg.flags);
                    };
                };
            };
        };
    };
}
