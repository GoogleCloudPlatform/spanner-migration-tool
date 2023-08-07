---
layout: default
title: CLI
nav_order: 1
has_children: true
parent: Operation Mode
permalink: /operation-mode/cli
---

# SMT Operation modes
{: .no_toc }

Spanner migration tool CLI follows [subcommands](https://github.com/google/subcommands)
structure with the the following general syntax:

```sh
spanner-migration-tool <subcommand> flags
```

The command `spanner-migration-tool help` displays the available subcommands.

```text
    commands   list all subcommand names
    help   describe subcommands and their syntax
```

To get help on individual subcommands, use

```sh
    spanner-migration-tool help <subcommand>
```

This will print the usage pattern, a few examples, and a list of all available subcommand flags.
