# File: ./1_create_collection.py
from weaviate.classes.config import Property, DataType, Configure
from helpers import CollectionName, connect_to_weaviate
import h5py
import json
from tqdm import tqdm
import numpy as np


def create_collection():
    with connect_to_weaviate() as client:

        # Delete existing collection if it exists
        client.collections.delete(CollectionName.SUPPORTCHAT)

        # Create a new collection with specified properties and vectorizer configuration
        chunks = client.collections.create(
            name=CollectionName.SUPPORTCHAT,
            properties=[
                Property(name="text", data_type=DataType.TEXT),
                Property(name="dialogue_id", data_type=DataType.INT),
                Property(name="company_author", data_type=DataType.TEXT),
                Property(name="created_at", data_type=DataType.DATE),
            ],
            vector_config=[
                Configure.Vectors.text2vec_cohere(
                    name="default",
                    source_properties=["text", "company_author"],
                    model="embed-multilingual-light-v3.0",
                    quantizer=Configure.VectorIndex.Quantizer.rq(),
                ),
            ],
            generative_config=Configure.Generative.cohere(model="command-r"),
        )

        assert client.collections.exists(CollectionName.SUPPORTCHAT)


def import_from_hdf5(file_path: str):

    with connect_to_weaviate() as client:

        collection = client.collections.get(CollectionName.SUPPORTCHAT)

        # Open the HDF5 file
        with h5py.File(file_path, "r") as hf:
            # Get the total number of objects for the progress bar
            total_objects = len(hf.keys())

            # Use batch import for efficiency
            with client.batch.fixed_size(batch_size=200) as batch:
                for uuid in tqdm(
                    hf.keys(), total=total_objects, desc="Importing objects"
                ):
                    group = hf[uuid]

                    # Get the object properties
                    properties = json.loads(group["object"][()])

                    # Get the vector(s)
                    vectors = {}
                    for key in group.keys():
                        if key.startswith("vector_"):
                            vector_name = key.split("_", 1)[1]
                            vectors[vector_name] = np.asarray(group[key])


                    # Add the object to the batch
                    batch.add_object(
                        collection=CollectionName.SUPPORTCHAT,
                        uuid=uuid,
                        properties=properties,
                        vector={"default": vectors["text_with_metadata"]},
                    )

    print(f"Import completed. {total_objects} objects imported.")
    if len(client.batch.failed_objects) > 0:
        print("*" * 80)
        print(f"***** Failed to add {len(client.batch.failed_objects)} objects *****")
        print("*" * 80)
        print(client.batch.failed_objects[:3])


create_collection()
import_from_hdf5("data/twitter_customer_support_cohere_50000.h5")
