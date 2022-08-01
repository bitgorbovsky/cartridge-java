# Java driver for Tarantool Cartridge

[![java-driver:ubuntu/master Actions
Status](https://github.com/tarantool/cartridge-java/workflows/ubuntu-master/badge.svg)](https://github.com/tarantool/cartridge-java/actions)

Java driver for Tarantool Cartridge for Tarantool versions 1.10+ based on the
asynchronous [Netty](https://netty.io) framework and official
[MessagePack](https://github.com/msgpack/msgpack-java) serializer.  Provides
CRUD APIs for seamlessly working with standalone Tarantool servers and clusters
managed by [Tarantool Cartridge](https://github.com/tarantool/cartridge) with
sharding via [vshard](https://github.com/tarantool/vshard).

## Quickstart

You can check [basic examples](https://github.com/tarantool/cartridge-java/blob/master/examples/)
directory in this repository. Each example contains Java project, Lua code for
Tarantool, and readme explaining step-by-step how to build and run example.

1. [Single client to simple Tarantool instance](examples/single_client_to_tarantool/README.md).
1. [Cluster client to Cartridge application](examples/cluster_client_to_cartridge/README.md).
1. [Cluster proxy client to Cartridge application](examples/proxy_client_to_cartrdge/README.md).
1. [Retrying proxy client to Cartridge application](examples/retrying_client_to_cartirgde/README.md).
1. [A quick guide](examples/QUICKGUIDE.md)

## Useful articles (in Russian)

- https://habr.com/ru/company/vk/blog/569926/

## Documentation

The Java Docs are available at [Github
pages](https://tarantool.github.io/cartridge-java/).

If you have any questions about working with Tarantool, check out the site [tarantool.io](https://tarantool.io/).

Feel free to ask questions about Tarantool and usage of this driver on Stack
Overflow with tag [tarantool](https://stackoverflow.com/questions/tagged/tarantool)
or join our community support chats in Telegram: [English](https://t.me/tarantool) and
[Russian](https://t.me/tarantoolru).

## [Changelog](https://github.com/tarantool/cartridge-java/blob/master/CHANGELOG.md)

## [License](https://github.com/tarantool/cartridge-java/blob/master/LICENSE)

## Requirements

Java 1.8 or higher is required for building and using this driver.

## Building

1. Docker accessible to the current user is required for running integration
   tests.
2. Set up the right user for running Tarantool with in the container: 
```bash
export TARANTOOL_SERVER_USER=<current user>
export TARANTOOL_SERVER_GROUP=<current group>
```
Substitute the user and group in these commands with the user and group under
which the tests will run.

3. Use `./mvnw verify` to run unit tests and `./mvnw test -Pintegration` to run
   integration tests.
4. Use `./mvnw install` for installing the artifact locally.

## Contributing

Contributions to this project are always welcome and highly encouraged. 
[See conventions for tests](docs/test-convention.md)
