#pragma once

#include <string>
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_score_table.h"


namespace ROCKSDB_NAMESPACE {

	// value -> void*
	static void* EncodeValue(uint64_t* v) { return reinterpret_cast<void*>(v); }

	// void* -> value and delete
	static void DeleteValue(const Slice& key, void* value) {
		uint64_t* v = reinterpret_cast<uint64_t*>(value);
		delete v;
	}


	class KeyUpdLru {
	private:
		std::shared_ptr<Cache> sstid_cache_;
	public:
		KeyUpdLru(const int kCacheSize) : sstid_cache_(NewLRUCache(kCacheSize)) {

		}
		~KeyUpdLru() {}

		// add <key, SST_id> pair to updmap
		// key is userkey
		// if key already exists, the old SST_id score+1
		void Add(Slice& key, uint64_t SST_id, std::shared_ptr<ScoreTable> score_tbl) {
			uint64_t* v_ptr = new uint64_t(SST_id);
			Cache::Handle* handle = sstid_cache_->Lookup(key);
			if (handle != nullptr) { //find key, so add score
				uint64_t* id_ptr = reinterpret_cast<uint64_t*>(sstid_cache_->Value(handle));
				//old id score +1
				score_tbl->AddScore(*id_ptr);
				sstid_cache_->Release(handle);
			}
			sstid_cache_->Insert(key, EncodeValue(v_ptr), 1, DeleteValue);
		}

		bool CompareAndUpdateSst(Slice& key, uint64_t old_id, uint64_t new_id) {
			bool successful = false;
			Cache::Handle* handle = sstid_cache_->Lookup(key);
			if (handle != nullptr) { //find key
				uint64_t* id_ptr = reinterpret_cast<uint64_t*>(sstid_cache_->Value(handle));
				if (old_id == *id_ptr) {
					sstid_cache_->Erase(key);
					uint64_t* v_ptr = new uint64_t(new_id);
					auto status = sstid_cache_->Insert(key, EncodeValue(v_ptr), 1, DeleteValue);
					successful = status.ok();
				}
				sstid_cache_->Release(handle);
			}
			return successful;
		}

		bool FindSst(const Slice& key, uint64_t* value) {

			Cache::Handle* handle = sstid_cache_->Lookup(key);
			if (handle != nullptr) { //find key
				uint64_t* id_ptr = reinterpret_cast<uint64_t*>(sstid_cache_->Value(handle));
				*value = *id_ptr;
				sstid_cache_->Release(handle);
				return true;
			}

			return false;
		}

	};
	
}
