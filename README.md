# Orchard

Orchard is a small Rust daemon that connects a machine to MQTT and exposes local system controls to Home Assistant using MQTT discovery.

Simply run `cargo run -- --help` to see all the options.

Current modules:

| Module         | Platforms             |
|----------------|-----------------------|
| Status         | All                   |
| Volume control | Windows, Linux, MacOS |
| Media control  | Linux only!           |

PRs porting existing functionalities to other platforms are welcome.
For new functionalities, better first ask in an issue.
