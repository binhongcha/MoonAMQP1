# Sources and licenses

- Original MoonBit protocol implementation based on the [OASIS AMQP 1.0 standard](https://www.oasis-open.org/standard/amqp/). Normative documents are referenced, not redistributed as source. No OASIS XML-to-code generator or copied protocol implementation is embedded.
- [Rhea 3.0.5](https://github.com/amqp/rhea), Apache-2.0: independent development-only codec and live-client oracle. Locked by `package-lock.json`; not a MoonBit runtime dependency. All message contents and test vectors in this repository are original synthetic data. Rhea's transitive npm packages retain their own licenses in the installed development tree.
- [moonbitlang/async 0.22.5](https://github.com/moonbitlang/async), Apache-2.0: native CLI stdin/stdout and file IO. Runtime protocol library and portable examples have no async dependency imports.
- `tools/windows` reuses the user's Apache-2.0 MoonORC/MoonEXR process-local compiler argv adapter. This auxiliary C/PowerShell code is excluded from effective MoonBit line totals. It does not replace the compiler implementation.

AI assisted interface design, implementation, testing and documentation. Generated public `.mbti` files are tool output from this project's sources. No private, commercial or unexplained upstream implementation was imported. Contest-specific reports and proposal facts remain outside tracked source.
