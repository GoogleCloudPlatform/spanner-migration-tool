---
layout: default
title: SMT CLI
nav_order: 4
has_children: true
permalink: /cli
---

# Spanner migration tool CLI
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
