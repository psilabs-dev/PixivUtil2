import argparse
import os
import shutil


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-dir", type=str, required=True)
    args = parser.parse_args()
    root_dir: str = args.root_dir
    assert os.path.exists(root_dir), f"Root directory {root_dir} does not exist."

    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        if not dirnames and not filenames:
            shutil.rmtree(dirpath)
            print(f"REMOVED: {dirpath}")

main()