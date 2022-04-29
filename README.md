[![Go Report Card](https://goreportcard.com/badge/github.com/cwithmichael/actionkv)](https://goreportcard.com/report/github.com/cwithmichael/actionkv)

# actionkv
Port of the Rust in Action ActionKV db from Rust to Go.

You can find the original Rust code [here](https://github.com/rust-in-action/code/tree/1st-edition/ch7/ch7-actionkv2). 

## How to use

Run `go build`. This should produce an executable named `actionkv`.
```
Usage:
        actionkv <file> get <key>
        actionkv <file> delete <key>
        actionkv <file> insert <key> <value>
        actionkv <file> update <key> <value>
```

Example:

![Screen Shot 2022-02-08 at 11 33 58 PM](https://user-images.githubusercontent.com/1703143/153128147-73440736-e868-4bb1-bbed-8531e7cdd7f6.png)
