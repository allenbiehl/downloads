import argparse
import uuid
import json
import random
from typing import Dict, List

class FileGenerator:

    def run(self, total_files: int, output_dir: str, ) -> str:
        for _file_id in range(total_files):
            total_messages = random.randint(1, 10)
            file_name = f"{uuid.uuid4().hex}"
            messages = []

            for _message_id in range(total_messages):
                messages.append({"name": "Event", "event_date": "2026-01-01"})

            self._write_file(file_path=f"{output_dir}/{file_name}.json", messages=messages)


    def _write_file(self, file_path: str, messages: List[Dict]) -> None:
        with open(file_path, mode="w", encoding="utf-8") as file:
            json.dump(messages, file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate files to output directory.")
    parser.add_argument("--total-files", type=int, required=True, help="Total files to generate")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory path")
    args = parser.parse_args()
    FileGenerator().run(args.total_files, args.output_dir)