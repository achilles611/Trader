# Toolchain restoration

The historical F4 archive and executable were found under the original temporary directory. F5 verified the official archive SHA-256 `02d98fc2c573793960ee06b7f642487d483fe30572f7e248804c207334a418d8` before extraction and the Anvil executable SHA-256 `c6e29da1b010fe00bac6c0dc5c29484bd641deb5a84050aea10d13e9dc4fe26f` before execution.

The stable installation is published atomically at the portable toolchain-root template `foundry/v1.8.1/982849d3140c01fd3b72905759581a132df7aa98/windows-amd64`. The receipt records local placement, version output, hash checks, and reparse checks. F4 code remains unchanged and discovers the stable binary through process `PATH`.

The historical temporary directory is preserved in this pass. F5 does not remove it until all enumerated source-content, process-use, and cleanup conditions are independently recorded.
