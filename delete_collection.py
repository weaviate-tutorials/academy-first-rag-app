"""
Delete Collection Script for Weaviate Academy

This script safely deletes the Movies collection from your Weaviate instance.
Use this when you need to rebuild your collection from scratch.

⚠️  WARNING: This will permanently delete all movie data in the collection!
"""

from helpers import CollectionName, connect_to_weaviate


def delete_movies_collection():
    """
    Delete the Movies collection from Weaviate
    """
    print("🗑️  Collection Deletion Script")
    print("=" * 40)
    print()

    try:
        # Connect to Weaviate
        print("🔌 Connecting to Weaviate...")
        with connect_to_weaviate() as client:
            print("✅ Connected successfully!")

            # Check if collection exists
            if client.collections.exists(CollectionName.MOVIES):
                print(f"📚 Found collection: {CollectionName.MOVIES}")
                print(f"📊 Collection size: {len(client.collections.get(CollectionName.MOVIES))} objects")
                print()

                # Confirm deletion
                print("⚠️  WARNING: This will permanently delete all movie data!")
                print("💡 This action cannot be undone.")
                print()

                confirm = input("Type 'DELETE' to confirm deletion: ")

                if confirm == "DELETE":
                    print("🗑️  Deleting collection...")
                    client.collections.delete(CollectionName.MOVIES)
                    print("✅ Collection deleted successfully!")
                    print()
                    print("💡 You can now run populate.py to recreate the collection.")
                else:
                    print("❌ Deletion cancelled.")
                    print("💡 Collection remains unchanged.")

            else:
                print(f"ℹ️  Collection '{CollectionName.MOVIES}' does not exist.")
                print("💡 Nothing to delete.")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Check your Weaviate connection and try again.")


if __name__ == "__main__":
    delete_movies_collection()
