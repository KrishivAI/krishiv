#!/usr/bin/env python3
"""Split krishiv-proto/src/lib.rs into ids, domain, wire, and tests."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "crates" / "krishiv-proto" / "src"
LIB = SRC / "lib.rs"

SECTIONS = [
    ("ids.rs", 10, 237),
    ("domain.rs", 239, 2883),
    ("wire.rs", 2885, 3994),
    ("proto_tests.rs", 3996, None),
]


def slice_lines(lines: list[str], start: int, end: int | None) -> str:
    return "".join(lines[start - 1 : end])


def main() -> None:
    lines = LIB.read_text().splitlines(keepends=True)

    for name, start, end in SECTIONS:
        body = slice_lines(lines, start, end)
        if name == "wire.rs":
            body = body.replace("pub mod wire {", "", 1).replace("super::", "crate::")
            if body.rstrip().endswith("}"):
                body = body.rstrip()[:-1] + "\n"
            content = (
                "#![forbid(unsafe_code)]\n"
                "//! Protobuf wire conversions.\n\n"
                "use std::error::Error;\n"
                "use std::fmt;\n\n"
                + body
            )
        elif name == "proto_tests.rs":
            content = body.replace("use super::", "use crate::").replace(
                "mod tests {", "mod proto_tests {", 1
            )
        elif name == "ids.rs":
            content = (
                "#![forbid(unsafe_code)]\n"
                "//! Identifier types.\n\n"
                "use std::error::Error;\n"
                "use std::fmt;\n\n"
                + body
            )
        else:
            content = "#![forbid(unsafe_code)]\n//! Domain control-plane contracts.\n\n" + body

        (SRC / name).write_text(content)
        print(f"wrote {name}")

    LIB.write_text(
        """#![forbid(unsafe_code)]

//! R2/R3 control-plane contracts for Krishiv.

mod ids;
mod domain;
pub mod wire;

pub use ids::*;
pub use domain::*;

#[cfg(test)]
mod proto_tests;
"""
    )
    print("wrote lib.rs")


if __name__ == "__main__":
    main()
