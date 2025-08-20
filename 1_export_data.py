import pandas as pd
from helpers import CollectionName, connect_to_weaviate

with connect_to_weaviate() as client:

    c = client.collections.get(CollectionName.MOVIES)

    buffer = []
    batch_size = 5000
    counter = 0
    batch_number = 0

    file_prefix = "movies_popular_w_vectors_"

    for o in c.iterator(include_vector=True):
        tmp_obj = {"properties": o.properties, "vectors": o.vector}
        buffer.append(tmp_obj)
        counter += 1

        if counter % batch_size == 0:
            # Convert buffer to DataFrame and save to parquet
            df = pd.DataFrame(buffer)
            filename = f"data/{file_prefix}{batch_number+1:02d}.parquet"
            df.to_parquet(filename, index=False)
            print(f"Saved {len(buffer)} records to {filename}")

            # Clear buffer and increment batch number
            buffer = []
            batch_number += 1

    # Save remaining records in the final batch
    if buffer:
        df = pd.DataFrame(buffer)
        filename = f"data/{file_prefix}{batch_number+1:02d}.parquet"
        df.to_parquet(filename, index=False)
        print(f"Saved {len(buffer)} records to {filename}")

    print(
        f"Total exported: {counter} records in {batch_number + (1 if buffer else 0)} batches"
    )
