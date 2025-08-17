import weaviate
import pandas as pd
from helpers import CollectionName

client = weaviate.connect_to_local()

c = client.collections.get(CollectionName.MOVIES)

buffer = []
batch_size = 5000
counter = 0
batch_number = 0

for o in c.iterator(include_vector=True):
    tmp_obj = {"properties": o.properties, "vectors": o.vector}
    buffer.append(tmp_obj)
    counter += 1

    if counter % batch_size == 0:
        # Convert buffer to DataFrame and save to parquet
        df = pd.DataFrame(buffer)
        filename = f"data/movies_dataset_w_vectors_{batch_number:04d}.parquet"
        df.to_parquet(filename, index=False)
        print(f"Saved {len(buffer)} records to {filename}")

        # Clear buffer and increment batch number
        buffer = []
        batch_number += 1

# Save remaining records in the final batch
if buffer:
    df = pd.DataFrame(buffer)
    filename = f"data/movies_dataset_w_vectors_{batch_number:04d}.parquet"
    df.to_parquet(filename, index=False)
    print(f"Saved {len(buffer)} records to {filename}")

client.close()
print(
    f"Total exported: {counter} records in {batch_number + (1 if buffer else 0)} batches"
)
