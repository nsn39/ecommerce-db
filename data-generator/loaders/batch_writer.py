import csv

def write_batches(records, output_path, batch_size=100000):
    with open(output_path, "w", newline="") as file:
        writer = None
        batch = []

        for record in records:
            batch.append(record)

            if len(batch) >= batch_size:
                if writer is None:
                    writer = csv.DictWriter(
                        file,
                        fieldnames=batch[0].keys()
                    )
                    writer.writeheader()

                writer.writerows(batch)
                batch.clear()

        if batch:
            if writer is None:
                writer = csv.DictWriter(
                    file,
                    fieldnames=batch[0].keys()
                )
                writer.writeheader()

            writer.writerows(batch)