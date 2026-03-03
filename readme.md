import csv
import os


def write_rows_stream(table_name, columns, row_iterator, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, f"{table_name}.csv")

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        for row in row_iterator:
            writer.writerow(row)