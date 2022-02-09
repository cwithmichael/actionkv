# actionkv
Port of the Rust in Action ActionKV db from Rust to Go.

You can find the original Rust code [here](https://github.com/rust-in-action/code/tree/1st-edition/ch7/ch7-actionkv2). 

## How to use

Run `go build`. This should produce an executable named `actionkv`.
```
Usage:
		actionkv FILE get KEY
		actionkv FILE delete KEY
		actionkv FILE insert KEY VALUE
		actionkv FILE update KEY VALUE
```

Example:

![Screen Shot 2022-02-08 at 11 33 58 PM](https://user-images.githubusercontent.com/1703143/153128147-73440736-e868-4bb1-bbed-8531e7cdd7f6.png)
