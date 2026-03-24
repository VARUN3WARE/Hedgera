#!/usr/bin/env python3
"""
Clear MongoDB Script
Deletes all documents from the market_data_1min collection in finrl_trading database.
Use this to reset historical data before running fresh backfill.
"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv

def clear_mongodb():
    """Clear all data from MongoDB market_data_1min collection"""
    try:
        # Load environment variables
        load_dotenv()
        mongo_uri = os.getenv('MONGODB_URI_STREAMING')
        
        if not mongo_uri:
            print("❌ MONGODB_URI_STREAMING not found in .env file")
            return False
        
        print("=" * 60)
        print("🗑️  CLEARING MONGODB DATA")
        print("=" * 60)
        
        # Connect to MongoDB
        print("📡 Connecting to MongoDB Atlas...")
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        db = client['finrl_trading']
        collection = db['market_data_1min']
        
        # Get current count
        before_count = collection.count_documents({})
        print(f"📊 Current documents: {before_count:,}")
        
        if before_count == 0:
            print("✅ MongoDB is already empty")
            client.close()
            return True
        
        # Confirm deletion
        print(f"\n⚠️  WARNING: About to delete {before_count:,} documents")
        response = input("Type 'yes' to confirm deletion: ")
        
        if response.lower() != 'yes':
            print("❌ Deletion cancelled")
            client.close()
            return False
        
        # Delete all documents
        print("\n🗑️  Deleting documents...")
        result = collection.delete_many({})
        
        # Verify deletion
        after_count = collection.count_documents({})
        
        print("\n" + "=" * 60)
        print("✅ MONGODB CLEARED SUCCESSFULLY")
        print("=" * 60)
        print(f"📊 Documents deleted: {result.deleted_count:,}")
        print(f"📊 Documents remaining: {after_count:,}")
        print("=" * 60)
        
        client.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error clearing MongoDB: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = clear_mongodb()
    exit(0 if success else 1)
