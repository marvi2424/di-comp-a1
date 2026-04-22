#!/usr/bin/env python3
"""
format_output.py  <raw_mrjob_output>  <output_file>

Reads the raw tab-separated mrjob output from Step 3 and writes clean output.txt.
Lines arrive sorted by key:
  - Category lines first (alphabetical), value = "Category word1:val ..."
  - '~DICT~' line last, value = "word1 word2 ..."

The script strips keys and JSON encoding, writing only the values.
"""
import sys
import json


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <raw_input> <output_file>")
        sys.exit(1)

    raw_path, out_path = sys.argv[1], sys.argv[2]
    pairs = []

    with open(raw_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            if '\t' not in line:
                continue
            key_json, value_json = line.split('\t', 1)
            key = json.loads(key_json)
            value = json.loads(value_json)
            pairs.append((key, value))

    # Sort by key so categories come out alphabetical and '~DICT~' last
    # (tilde ASCII 126 > all uppercase letters). Safety net in case the
    # pipeline used multiple reducers and partitions weren't merged sorted.
    pairs.sort(key=lambda kv: kv[0])

    with open(out_path, 'w', encoding='utf-8') as f:
        for _, value in pairs:
            f.write(value + '\n')


if __name__ == '__main__':
    main()
